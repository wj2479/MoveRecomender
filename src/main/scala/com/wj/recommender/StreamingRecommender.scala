package com.wj.recommender

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import com.wj.recommender.DataLoader.MongoConfig
import com.wj.recommender.OfflineRecommender.MovieRecs
import com.wj.recommender.StatisticsRecommender.MongoConfig
import com.wj.utils.JedisConnectionPool
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 实时推荐服务
 */
object StreamingRecommender {

  /**
   * @param uri MongDB 的连接
   * @param db  MongDB 的 数据库
   */
  case class MongoConfig(uri: String, db: String)

  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://guest:123456@hadoop002:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val conf = new SparkConf().setMaster(config("spark.cores")).setAppName(getClass.getSimpleName)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(conf, Seconds(2))

    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import spark.implicits._

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据：电影相似度矩阵数据，转换成为 Map[Int, Map[Int, Double]]，把它广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map(movieRecs =>
        (movieRecs.movieId, movieRecs.recs.map(x => (x.movieId, x.rating)).toMap)
      )
      .collectAsMap()

    val simMovieMatrixBroadCast = spark.sparkContext.broadcast(simMovieMatrix)

    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "hadoop001:9092,hadoop002:9092,hadoop003:9092";
    val topics = Array(config("kafka.topic"))
    val group = "recommender"

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) // 是否自动提交(间隔5秒)偏移量，设置为false 手动调焦
    )

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaParams))

    // 把原始数据 uid|mid|score|timestamp 转换成评分流
    val ratingStream = dstream.map(msg => {
      val attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    })

    // 继续做流式处理，实时算法部分
    ratingStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach {
          case (uid, mid, score, timestamp) => {
            println("rating data coming! >>>>>>>>>>>>>>>>>>>>")

            // 1、从 redis 中获取当前用户最近的 K 次评分，保存成 Array[(mid, score)]
            val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, uid, JedisConnectionPool.getConnection())

            // 2、从相思度矩阵中取出当前电影最相似的 N 个电影，作为备选电影列表，Array[mid]
            val candidateMovies = getTopsSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            // 3、对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
            val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

            // 4、把推荐数据保存到 MongoDB 中
            storeDataInMongDB(uid, streamRecs)
          }
        }
      }
    })

    // 开始接收和处理数据
    ssc.start()
    ssc.awaitTermination()
  }

  // 因为 redis 操作返回的是 java 类，为了使用 map 操作需要引入转换类

  import scala.collection.JavaConversions._

  /**
   * 获取当前最近的 K 次电影评分
   *
   * @param num 评分的个数
   * @param uid 谁的评分
   * @return
   */
  def getUserRecentlyRatings(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从 redis 中读取数据，用户评分数据保存在 uid:UID 为 key 的队列中，里面的 value 是 MID:SCORE
    jedis.lrange("uid:" + uid.toString, 0, num) // 从用户的队列中取出 num 个评分
      .map {
        item => // 具体的每一个评分是以冒号分割的两个值
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
   * 获取与当前电影 K 个相似的电影，作为备选电影
   *
   * @param num         相似电影的数量
   * @param mid         当前电影的 ID
   * @param uid         当前的评分用户 ID
   * @param simMovies   电影相似度矩阵的广播变量值
   * @param mongoConfig MongoDB 的配置
   * @return 过滤之后的备选电影列表
   */
  def getTopsSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                      (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 1、从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 2、从 MongnDB 中查询用户已经看过的电影
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    val ratingExist = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item => item.get("mid").toString.toInt
      }

    // 3、把看过的过滤掉，得到备选电影的输出列表
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
   * 计算待选电影的推荐分数
   *
   * @param candidateMovies     与当前电影最相似的 N 个电影（待选电影）
   * @param userRecentlyRatings 用户最近的 K 次评分
   * @param simMovies           电影相似度矩阵的广播变量值
   * @return
   */
  def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // 定义一个 ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义一个 HashMap，保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (candidateMovies <- candidateMovies; userRecentlyRatings <- userRecentlyRatings) {

      // 获取备选电影和最近评分电影的相似度的得分
      val simScore = getMoviesSimScore(candidateMovies, userRecentlyRatings._1, simMovies)

      if (simScore > 0.7) {
        // 计算候选电影的基础推荐得分
        scores += ((candidateMovies, simScore * userRecentlyRatings._2))
        if (userRecentlyRatings._2 > 3) {
          increMap(candidateMovies) = increMap.getOrDefault(candidateMovies, 0) + 1
        } else {
          decreMap(candidateMovies) = decreMap.getOrDefault(candidateMovies, 0) + 1
        }
      }
    }

    // 根据备选电影的 mid 做 groupBy，根据公式求最后的推荐得分
    scores.groupBy(_._1).map {
      // groupBy 之后得到的数据是 Map(mid -> ArrayBuffer[(mid, score)])
      case (mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray
  }

  /**
   * 获取备选电影和最近评分电影的相似度的得分
   *
   * @param mid1      备选电影
   * @param mid2      最近评分电影
   * @param simMovies 电影相似度矩阵的广播变量值
   * @return
   */
  def getMoviesSimScore(mid1: Int, mid2: Int,
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  /**
   * 求一个数的对数，底数默认为 10
   *
   * @param m
   * @return
   */
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  /**
   * 把结果写入对应的 MongoDB 表中
   *
   * @param uid
   * @param streamRecs  流式的推荐结果
   * @param mongoConfig MongoDB 的配置
   */
  def storeDataInMongDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义到 MongoDB 中 StreamRecs 表的连接
    val streamRecsCollection = mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    // 如果表中已有 uid 对应的数据，则删除
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // 将新的 streamRecs 存入表 StreamRecs 中
    streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }
}

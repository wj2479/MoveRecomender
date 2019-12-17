package com.wj.recommender

import com.wj.recommender.DataLoader.MongoConfig
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/**
 * 离线推荐服务
 */
object OfflineRecommender {
  // 定义 MongoDB 数据库中的一些表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 基于评分数据的 LFM，只需要 rating 数据（用户评分表）注意：spark mllib 中有 Rating 类，为了便于区别，我们重新命名为 MovieRating
  case class MovieRating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

  // 标准推荐对象
  case class Recommendation(movieId: Int, rating: Double)

  // 用户推荐列表
  case class UserRecs(userId: Int, recs: Seq[Recommendation])

  // 电影相似度（电影推荐）
  case class MovieRecs(movieId: Int, recs: Seq[Recommendation])

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  /**
   * 用户最大推荐数量
   */
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://guest:123456@hadoop002:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val spark = SparkSession.builder().appName(getClass.getSimpleName).master(config("spark.cores")).getOrCreate()

    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import spark.implicits._
    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    /** ******************************计算用户电影推荐矩阵 ********************************/

    // 从 MongoDB 中加载数据
    val movieRatingRdd = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.userId, rating.movieId, rating.rating)) // 转换成 RDD，并且去掉时间戳
      .cache()

    // 构建训练数据集
    val trainData = movieRatingRdd.map(x => Rating(x._1, x._2, x._3))

    // 参数(
    //  特征数量,如果这个值太小拟合的就会不够，误差就很大；如果这个值很大，就会导致模型大泛化能力较差；
    //  循环次数iter，这个设置的越大肯定是越精确，但是设置的越大也就意味着越耗时；
    //  正则因子（推荐值为0.01），如果设置很大就可以防止过拟合问题，如果设置很小，其实可以理解为直接设置为0，那么就不会有防止过拟合的功能了；
    // )
    val (rank, iterations, lambda) = (50, 5, 0.01)

    // 训练隐语义模型
    val model = ALS.train(trainData, rank, iterations, lambda)

    // 从 ratingRDD 数据中提取所有的 uid 和 mid ，并去重
    val userRDD = movieRatingRdd.map(_._1).distinct()
    val movieRDD = movieRatingRdd.map(_._2).distinct()

    // 基于用户和电影的隐特征，计算预测评分，得到用户推荐列表
    // user 和 movie 做笛卡尔积，得到一个空评分矩阵，即产生 (uid，mid) 的元组
    val userMovies = userRDD.cartesian(movieRDD)
    // 调用 model 的 predict 方法进行预测评分
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings.filter(_.rating > 0) // 过滤评分大于0的数据
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, iter) => {
          val recs = iter.toList.sortWith((a, b) => a._2 > b._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2))
          UserRecs(userId, recs)
        }
      }.toDF()

    //    userRecs.show()

    // 把结果写入对应的 MongoDB 表中
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    /** ******************************计算电影相似度矩阵 ********************************/

    // 基于电影的隐特征，计算相似度矩阵，得到电影的相似度列表
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { // 过滤自己跟自己的笛卡尔积
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > 0.6) // 过滤出相似度大于0.6的
      .groupByKey()
      .map {
        case (mid, recs) => MovieRecs(mid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    // 把结果写入对应的 MongoDB 表中
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 求两个向量的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2()) // l1范数：向量元素绝对值之和；l2范数：即向量的模长（向量的长度）,向量元素的平方和再开方
  }
}

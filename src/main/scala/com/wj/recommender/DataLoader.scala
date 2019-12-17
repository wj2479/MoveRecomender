package com.wj.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
 * 数据预处理，加载到Mongodb和ES
 */
object DataLoader {

  /**
   * 在项目启动前设置一下的属性，防止报错
   * 解决netty冲突后初始化client时还会抛出异常
   * java.lang.IllegalStateException: availableProcessors is already set to [4], rejecting [4]
   */
  System.setProperty("es.set.netty.runtime.available.processors", "false");

  /**
   * 电影样例类
   *
   * @param movieId 电影ID
   * @param title   电影标题
   * @param genres  电影类型  以|分割
   */
  case class Movie(movieId: Int, title: String, genres: String)

  /**
   * 用户评分样例类
   *
   * @param userId    用户ID
   * @param movieId   电影ID
   * @param rating    评分数值
   * @param timestamp 评分时间
   */
  case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

  /**
   * 用户标签样例类
   *
   * @param userId    用户ID
   * @param movieId   电影ID
   * @param tag       电影标签
   * @param timestamp 评分时间
   */
  case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long)

  // 把 MongoDB 和 Elasticsearch 的配置封装成样例类
  /**
   * @param uri MongDB 的连接
   * @param db  MongDB 的 数据库
   */
  case class MongoConfig(uri: String, db: String)

  /**
   * @param httpHosts      ES 的 http 主机列表，逗号分隔
   * @param transportHosts ES 的 http 端口列表，逗号分隔
   * @param index          需要操作的索引库，即数据库
   * @param clusterName    集群名称：默认是 my-application
   */
  case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

  // 定义 MongoDB 数据库中的一些表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  // 定义 ES 中的一些索引（即数据库）
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    // 定义用到的配置参数
    val config = Map(
      "mongo.uri" -> "mongodb://guest:123456@hadoop002:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop001:9200",
      "es.transportHosts" -> "hadoop001:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-application"
    )

    val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._
    // 直接读取csv文件
    val movieDF: Dataset[Movie] = spark.read.option("header", true).csv("./src/main/resources/movies.csv").map(row =>
      Movie(row(0).toString.toInt, row(1).toString, row(2).toString))
    //    movieDF.show()

    val ratingDF: Dataset[Rating] = spark.read.option("header", true).csv("./src/main/resources/ratings.csv").map(row =>
      Rating(row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble, row(3).toString.toLong))
    //    ratingDF.show()

    val tagDF: Dataset[Tag] = spark.read.option("header", true).csv("./src/main/resources/tags.csv").map(row =>
      Tag(row(0).toString.toInt, row(1).toString.toInt, row(2).toString, row(3).toString.toLong))
    //    tagDF.show()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 将数据保存到 MongoDB 中
    storeDataInMongDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._
    // 数据预处理，把 movie 对应的 tag 信息添加进去，加一列，使用 “|” 分隔：tag1|tag2|...
    /**
     * mid,tags
     * tags: tag1|tag2|tag3|...
     */
    val newTags = tagDF.groupBy("movieId").agg(concat_ws("|", collect_set($"tag")) as "tags").select("movieId", "tags")

    // 将数据与标签进行左连接
    val movieWithTagsDF = movieDF.join(newTags, Seq("movieId"), "left")

    // 声明一个隐式的配置对象
    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    // 将数据保存到 ES 中
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  /**
   * 将数据 写入到mongodb中
   *
   * @param movieDF
   * @param ratingDF
   * @param tagDF
   * @param mongoConfig
   */
  def storeDataInMongDB(movieDF: Dataset[Movie], ratingDF: Dataset[Rating], tagDF: Dataset[Tag])(implicit mongoConfig: MongoConfig): Unit = {

    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果 MongoDB 中已有相应的数据库，则先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 将电影数据写入到电影表
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    // 电影评分表
    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    //电影标签表
    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    // 对 MongoDB 中的数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }


  /**
   * 存储数据到ES
   *
   * @param movieWithTagsDF
   * @param esConfig
   */
  def storeDataInES(movieWithTagsDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // 新建一个 es 的配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName)
      .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中.build()
      .build()
    // 新建一个 es 的客户端
    val esClient = new PreBuiltTransportClient(settings)
    // 需要将 TransportHosts 添加到 esClient 中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // 需要先清除掉 ES 中遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    // 将数据写入到 ES 中
    movieWithTagsDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "movieId") // 映射主键
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }
}

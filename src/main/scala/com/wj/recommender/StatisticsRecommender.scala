package com.wj.recommender

import java.text.SimpleDateFormat
import java.util.Date

import com.wj.recommender.DataLoader.{Movie, Rating}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 离线统计服务
 */
object StatisticsRecommender {

  // 把 MongoDB 和 Elasticsearch 的配置封装成样例类
  /**
   * @param uri MongDB 的连接
   * @param db  MongDB 的 数据库
   */
  case class MongoConfig(uri: String, db: String)

  case class Recommendation(mid: Int, score: Double)

  // 定义电影类别 top10 推荐对象（每种类型的电影集合中评分最高的 10 个电影）
  case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

  // 定义 MongoDB 数据库中的一些表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies" // 电影评分个数统计表
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" // 最近电影评分个数统计表
  val AVERAGE_MOVIES_Score = "AverageMoviesScore" // 电影平均评分表
  val GENRES_TOP_MOVIES = "GenresTopMovies" // 电影类别 TOP10

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://guest:123456@hadoop002:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 SparkConf 对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName(getClass.getSimpleName)

    // 创建一个 SparkSession 对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import spark.implicits._

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 从 MongoDB 中加载数据
    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie] // DataSet
      .toDF()

    // 从 MongoDB 中加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating] // DataSet
      .toDF()

    // 创建临时表，名为 ratings
    ratingDF.createOrReplaceTempView("ratings")

    // 1、历史热门电影统计：根据所有历史评分数据，计算历史评分次数最多的电影。mid,count
    val rateMoreMoviesDF = spark.sql("select movieId, count(movieId) as count from ratings group by movieId")
    // 把结果写入对应的 MongoDB 表中
    storeDFInMongDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2、最近热门电影统计：根据评分次数，按月为单位计算最近时间的月份里面评分次数最多的电影集合。mid,count,yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册一个 UDF 函数，用于将 timestamp 转换成年月格式    1260759144000 => 201605
    spark.udf.register("changeDate", (x: Long) => {
      simpleDateFormat.format(new Date(x * 1000)).toInt
    })
    // 对原始数据 ratings 做预处理，去掉 uid，保存成临时表，名为 ratingOfMonth
    val ratingOfYearMonth = spark.sql("select movieId, rating, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    // 根据评分次数，按月为单位计算最近时间的月份里面评分次数最多的电影集合
    val rateMoreRecentlyMoviesDF = spark.sql("select movieId, count(movieId), yearmonth as count from ratingOfMonth group by yearmonth, movieId order by yearmonth desc, count desc")
    // 把结果写入对应的 MongoDB 表中
    storeDFInMongDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3、电影平均得分统计：根据历史数据中所有用户对电影的评分，周期性的计算每个电影的平均得分。mid,avg
    val averageMoviesDF = spark.sql("select movieId, avg(rating) as avg from ratings group by movieId")
    // 把结果写入对应的 MongoDB 表中
    storeDFInMongDB(averageMoviesDF, AVERAGE_MOVIES_Score)

    // 4、每个类别优质电影统计：根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的 10 个电影。
    // 定义所有的类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Famil y", "Fantasy",
      "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
    // 把电影的平均评分加入到 movie 表中，使用 inner join，不满足条件的不显示
    val movieWithScore = movieDF.join(averageMoviesDF, "movieId")
    // 为做笛卡尔积，我们需要把 genres 转成 RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)
    // 计算类别 top10，首先对类别和电影做笛卡尔积，然后进行过滤
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        // 条件过滤：找出 movie 中的字段 genres 值包含当前类别 genre 的那些
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase())
      }
      .map { // 将整个数据集的数据量减小，生成 RDD[String, Iter[mid, avg]]
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("movieId"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item =>
          Recommendation(item._1, item._2)
        ))
      }
      .toDF()
    // 把结果写入对应的 MongoDB 表中
    storeDFInMongDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    // 关闭 SparkSession
    spark.stop()
  }

  /**
   * 将DataFrame写入到mongodb
   *
   * @param df
   * @param collection_name
   * @param mongoConfig
   */
  def storeDFInMongDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
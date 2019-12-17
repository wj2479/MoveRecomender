package com.wj.recommender

import breeze.numerics.sqrt
import com.wj.recommender.DataLoader.MongoConfig
import com.wj.recommender.OfflineRecommender.MovieRating
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://guest:123456@hadoop002:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 SparkConf 对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // 创建一个 SparkSession 对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import spark.implicits._

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 从 MongoDB 中加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating] // DataSet
      .rdd
      .map(rating => Rating(rating.userId, rating.movieId, rating.rating)) // 转换成 RDD，并且去掉时间戳
      .cache()

    // 将一个 RDD 随机切分成两个 RDD，用以划分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // 模型参数选择，输出最优参数
    adjustALSParam(trainingRDD, testingRDD)

    spark.close()
  }

  /**
   * 计算最优参数
   *
   * @param trainData
   * @param testData
   */
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 这里指定迭代次数为 5，rank 和 lambda 在几个值中选取调整
    val result = for (rank <- Array(50, 100, 200, 300); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {     // 将for循环中执行的结果保存到集合中，结束后返回
        val model: MatrixFactorizationModel = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // 控制台打印输出
    // println(result.sortBy(_._3).head)
    println(result.minBy(_._3))
  }

  // 计算模型的均方根误差
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 计算预测评分
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // 以 uid,mid 作为外键，将 实际观测值 和 预测值 使用内连接
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predicted = predictRating.map(item => ((item.user, item.product), item.rating))
    // 内连接，得到 (uid, mid), (observe, predict)

    // 计算 RMSE
    sqrt(
      observed.join(predicted).map {
        case ((uid, mid), (observe, predict)) =>
          val err = observe - predict
          err * err
      }.mean()
    )
  }
}

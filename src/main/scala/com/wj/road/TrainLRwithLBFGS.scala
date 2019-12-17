package com.wj.road

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.wj.utils.JedisConnectionPool
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * 使用逻辑回归 训练模型
 */
object TrainLRwithLBFGS {

  val dayFormat = new SimpleDateFormat("yyyyMMdd")
  val minuteFormat = new SimpleDateFormat("HH:mm")

  val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val pool = JedisConnectionPool.getConnection()
    pool.select(2)

    // 要预测的道路ID
    val camera_ids = List("310999003001", "310999003102")
    // 道路的关联关系， 代表与这个道路有交集的道路的ID
    val camera_relations: Map[String, Array[String]] = Map[String, Array[String]](
      "310999003001" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"),
      "310999003102" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204")
    )

    camera_ids.map(camera_id => {
      val hours = 5 // 根据前5个小时的数据 生成模型
      val nowtimelong = System.currentTimeMillis();
      val now = new Date(nowtimelong)
      val day = dayFormat.format(now) //yyyyMMdd

      // 获取模型相关的道路ID
      val array: Array[String] = camera_relations.get(camera_id).get

      /**
       * relations中存储了每一个卡扣在day这一天每一分钟的平均速度
       */
      val relations: Array[(String, util.Map[String, String])] = array.map({ camera_id =>
        val minute_speed_car_map = pool.hgetAll(day + "_'" + camera_id + "'")
        (camera_id, minute_speed_car_map)
      })

      val dataSet = ArrayBuffer[LabeledPoint]()
      relations.foreach(println)

      // 循环  从300到0  递减  不包含0
      for (i <- Range(60 * hours, 0, -1)) {
        // 特征数组   就是 X1  X2  X3 ...
        val features = ArrayBuffer[Double]()
        // 标签数组  也就是Y值
        val labels = ArrayBuffer[Double]()

        // 当前时间 到I的时间 及前两分钟的值
        for (index <- 0 to 2) {
          val tempOne = nowtimelong - 60 * 1000 * (i - index) // 当前时间到现在的那一分钟
          val d = new Date(tempOne)
          val tempMinute = minuteFormat.format(d) //HHmm

          //下一分钟
          val tempNext = tempOne - 60 * 1000 * (-1)
          val dNext = new Date(tempNext)
          val tempMinuteNext = minuteFormat.format(dNext) //HHmm

          for ((k, v) <- relations) {
            val map = v //map -- k:HHmm    v:Speed
            // 如果是第三分钟，获取下一分钟的拥堵状态  作为这三分钟的数据的Y值
            if (index == 2 && k == camera_id) {
              if (map.containsKey(tempMinuteNext)) {
                val info = map.get(tempMinuteNext).split("_")
                val f = info(0).toFloat / info(1).toFloat
                labels += f
              }
            }
            if (map.containsKey(tempMinute)) {
              val info = map.get(tempMinute).split("_")
              val f = info(0).toFloat / info(1).toFloat
              features += f
            } else {
              features += -1.0
            }
          }

          if (labels.toArray.length == 1) {
            //array.head 返回数组第一个元素
            val label = (labels.toArray).head
            // 将数据返回为11类
            val record = LabeledPoint(if ((label.toInt / 10) < 10) (label.toInt / 10) else 10.0, Vectors.dense(features.toArray))
            // 将数据添加至数据集
            dataSet += record
          }
        }
      }
      dataSet.foreach(println)

      // 构建RDD
      val data: RDD[LabeledPoint] = sc.parallelize(dataSet)
      //将data这个RDD随机分成 8:2两个RDD
      val splits = data.randomSplit(Array(0.8, 0.2))
      //构建训练集
      val training = splits(0)
      /**
       * 测试集的重要性：
       * 测试模型的准确度，防止模型出现过拟合的问题
       */
      val test = splits(1)

      if (!data.isEmpty()) {
        val model = new LogisticRegressionWithLBFGS()
          .setNumClasses(11) // 设置分类的数量
          .setIntercept(true) // 设置截距
          .run(training)

        val predictMap = test.map {
          case LabeledPoint(label, features) =>
            val prediction = model.predict(features)
            (prediction, label)
        }
        predictMap.foreach(x => println("预测值:" + x._1 + "真实值：" + x._2))

        // Get evaluation metrics. 得到评价指标
        val metrics: MulticlassMetrics = new MulticlassMetrics(predictMap)
        val precision = metrics.precision// 准确率
        println("Precision = " + precision)

      }
    })

    pool.close()
  }
}

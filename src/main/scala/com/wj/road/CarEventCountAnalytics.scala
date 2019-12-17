package com.wj.road

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wj.utils.JedisConnectionPool
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * 从kafka 消费数据 并统计实时的路况数据
 */
object CarEventCountAnalytics {

  def main(args: Array[String]): Unit = {
    val group = "car1"

    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(60))
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "hadoop001:9092,hadoop002:9092,hadoop003:9092";
    val topics = Array("car_events")

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) // 是否自动提交(间隔5秒)偏移量，设置为false 手动调焦
    )

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaParams))

    dstream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        // 处理逻辑
        val carAndSpeedRdd: RDD[(String, (Int, Int))] = rdd.map(text => {
          val json = JSON.parseObject(text.value())
          (json.getString("camera_id"), (json.getIntValue("speed"), 1))
        }).reduceByKey((a: Tuple2[Int, Int], b: Tuple2[Int, Int]) =>
          (a._1 + b._1, a._2 + b._2)
        )

        carAndSpeedRdd.foreach(pair => {
          val pool = JedisConnectionPool.getConnection()
          val camera_id = pair._1
          val speedTotal = pair._2._1
          val carCount = pair._2._2

          // 格式化日期
          val now = Calendar.getInstance().getTime
          val minuteFormat = new SimpleDateFormat("HH:mm")
          val dayFormat = new SimpleDateFormat("yyyyMMdd")

          val time = minuteFormat.format(now)
          val day = dayFormat.format(now)

          pool.select(2)
          pool.hset(day + "_" + camera_id, time, speedTotal + "_" + carCount)

          pool.close()
        }
        )

        // some time later, after outputs have completed
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

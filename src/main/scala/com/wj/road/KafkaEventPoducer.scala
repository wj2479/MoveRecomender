package com.wj.road

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 读取本地数据 写入到kafka  模拟实时的路况数据
 */
object KafkaEventPoducer {

  def main(args: Array[String]): Unit = {

    val topic = "car_events"
    val brokers = "hadoop001:9092,hadoop002:9092,hadoop003:9092"
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
//    props.put(ProducerConfig.ACKS_CONFIG, 0)

    val producer = new KafkaProducer[String, String](props)

    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val records: Array[Array[String]] = sc.textFile("./src/main/data/2014082013_all_column_test.txt").filter(!_.startsWith(";")).map(_.split(",")).collect()

    for (i <- 1 to 100) {
      for (record <- records) {
        val json = new JSONObject()
        json.put("camera_id", record(0))
        json.put("car_id", record(2))
        json.put("event_time", record(4))
        json.put("speed", record(6))
        json.put("road_id", record(13))
        val proRecord = new ProducerRecord[String, String](topic, json.toString)
        producer.send(proRecord)
        println("Msg Send->" + proRecord)
        Thread.sleep(200)
      }
    }
    sc.stop()
  }
}

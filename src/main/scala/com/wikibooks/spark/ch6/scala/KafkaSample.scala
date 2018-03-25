package com.wikibooks.spark.ch6.scala

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

// 6.2.4절 예제 6-10
object KafkaSample extends Serializable {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaSample")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val params = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "test-group-1",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")

    val topics = Array("test")

    val ds = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, params))

    ds.flatMap(record => record.value.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print

    ssc.start
    ssc.awaitTermination()
  }
}
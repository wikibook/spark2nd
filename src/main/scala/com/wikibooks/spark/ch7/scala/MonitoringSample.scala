package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.streaming.{ OutputMode, StreamingQuery, Trigger }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.streaming.StreamingQueryManager

object MonitoringSample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("KafkaSample")
      .master("local[*]")
      .getOrCreate()

    // StreamingQueryListener 를 이용한 모니터링 
    spark.streams.addListener(new SimpleListener())

    run(spark)
  }

  def run(spark: SparkSession) {

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "test")
      .load

    val query = df.writeStream
      .format("console")
      .start()

    // StreamingQuery를 이용한 모니터링 
    // monitor(query)

    query.awaitTermination()
    
    query.stop
  }

  def monitor(query: StreamingQuery) {
    new Thread(new Runnable {
      def run() {
        while (true) {
          println("query.lastProgress:" + query.lastProgress)
          query.explain(true)
          Thread.sleep(5 * 1000)
        }
      }
    }).start
  }
}
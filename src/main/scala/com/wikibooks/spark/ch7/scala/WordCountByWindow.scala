package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import java.util.Calendar
import java.sql.Timestamp
import java.util.Date

// 7.4.2절
object WordCountByWindow {

  def main(args: Array[String]): Unit = {

    // step1
    val spark = SparkSession
      .builder()
      .appName("WordCountByWindow")
      .master("local[*]")
      .getOrCreate()

    // step2
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // step3(port번호는 ncat 서버와 동일하게 수정해야 함)
    // includeTimestamp를 true로 설정한 뒤 시간값이 비정상적으로 조회될 경우
    // 타임스탬프를 from_unixtime(unix_timestamp('timestamp)*1000)과 같이 보정하여 사용
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", false)
      .load()
      .select('value as "words", current_timestamp as "ts")
      
    // step4
    val words = lines.select(explode(split('words, " ")).as("word"), window('ts, "10 minute", "5 minute").as("time"))
    val wordCount = words.groupBy("time", "word").count

    // step5
    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete)
      .option("truncate", false)
      .format("console")
      .start()
      
    // step6
    query.awaitTermination()
  }
}
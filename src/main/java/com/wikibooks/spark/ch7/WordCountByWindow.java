package com.wikibooks.spark.ch7;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.current_timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

// 7.4.2절
public class WordCountByWindow {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
    		.builder() 
        .appName("WordCountByWindow")
        .master("local[*]")
        .getOrCreate();

    // port번호는 ncat 서버와 동일하게 수정해야 함
    // includeTimestamp를 true로 설정한 뒤 시간값이 비정상적으로 조회될 경우
    // 타임스탬프를 from_unixtime(unix_timestamp(col("timestamp")).multiply(1000))과 같이 보정하여 사용  
    Dataset<Row> lines = spark
    .readStream()
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", false)
    .load()
    .select(col("value").as("words"), current_timestamp().as("ts"));
    
    Dataset<Row> words = lines.select(explode(split(col("words"), " ")).as("word"), window(col("ts"), "10 minute", "5 minute").as("time"));
    Dataset<Row> wordCount = words.groupBy("time", "word").count();
    
    StreamingQuery query = wordCount.writeStream()
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .format("console")
      .start();

    query.awaitTermination();    
  }
}

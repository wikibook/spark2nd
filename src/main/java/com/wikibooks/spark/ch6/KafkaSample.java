package com.wikibooks.spark.ch6;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaSample {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaSample");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

    Map<String, Object> params = new HashMap<>();
    params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    params.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-2");
    params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    List<String> topics = Arrays.asList("test");

    JavaInputDStream<ConsumerRecord<String, String>> ds = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, params));
    
    ds.flatMap((ConsumerRecord<String, String>  record) -> Arrays.asList(record.value().split(" ")).iterator())
    .mapToPair((String word) -> new Tuple2<String, Integer>(word, 1))
    .reduceByKey((Integer v1, Integer v2) -> v1 + v2)
    .print();
    
    ssc.start();
    ssc.awaitTermination();
  }
}
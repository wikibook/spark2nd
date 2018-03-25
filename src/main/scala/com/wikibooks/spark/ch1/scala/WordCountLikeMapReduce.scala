package com.wikibooks.spark.ch1.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext, SparkFiles }

// 1.2.5 (Map과 Reduce 단계로 나누어 단어수 세기 예제 구현해 보기)
object WordCountLikeMapReduce {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountTest")
    val sc = new SparkContext(conf)
    
    val rdd = sc.textFile("file:///Users/beginspark/apps/spark/README.md")

    // Map 단계: 각 라인을 공백 문자를 기준으로 단어로 분리하고 각 단어를 (단어, 단어수) 순서쌍으로 변환합니다. 
    val mappedRdd = rdd.flatMap(_.split(" ")).map(word => (word, 1))

    // 동일한 단어를 가진 데이터 요소들을 하나의 그룹으로 만듭니다. 
    val groupedRdd = mappedRdd.groupByKey

    // Reduce 단계 (같은 그룹에 속한 단어들을 모아서 한번에 처리합니다)
    val reducedRdd = groupedRdd.map {
      case (key, values) => {
        var count = 0
        for (value <- values) {
          count += value
        }
        (key, count)
      }
    }

    // 단어 "For" 의 개수 확인
    val count = reducedRdd.collectAsMap().get("For").get
    println(count)
  }
}
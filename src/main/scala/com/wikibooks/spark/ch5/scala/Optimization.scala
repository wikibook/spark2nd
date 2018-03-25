package com.wikibooks.spark.ch5.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Optimization {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Optimization")
      .master("local[*]")
      .getOrCreate()

    showPlan(spark, createDF(spark))
  }

  def createDF(spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.read.text("/Users/beginspark/Apps/spark/README.md")
      .selectExpr("split(value, ' ') as words ")
      .select(explode('words) as "word")
      .groupBy("word").count
      .where("word == 'Spark' ")
  }

  def showPlan(spark: SparkSession, df: DataFrame) {

    val qe = df.queryExecution

    //println(qe.logical)
    //println(qe.analyzed)
    //println(qe.optimizedPlan)
    //println(qe.sparkPlan)
    //println(qe.executedPlan)

    //println(df.explain(true)) 
  }
}
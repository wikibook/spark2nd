package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent

class SimpleListener extends StreamingQueryListener {

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    println("onQueryStarted: " + queryStarted.runId)
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    println("onQueryProgress:" + queryProgress.progress.prettyJson)
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    println("onQueryTerminated:" + queryTerminated.runId)
  }
}
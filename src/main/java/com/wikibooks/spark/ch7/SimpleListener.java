package com.wikibooks.spark.ch7;

import org.apache.spark.sql.streaming.StreamingQueryListener;

public class SimpleListener extends StreamingQueryListener {

	@Override
	public void onQueryStarted(QueryStartedEvent queryStarted) {
		System.out.println(queryStarted.runId());
	}

	@Override
	public void onQueryProgress(QueryProgressEvent queryProgress) {
		System.out.println(queryProgress.progress());
	}

	@Override
	public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
		System.out.println(queryTerminated.runId());
	}
}
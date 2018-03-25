package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class MonitoringSample {

	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder().appName("MonitoringSample").master("local[*]").getOrCreate();

		MonitoringSample obj = new MonitoringSample();

		// StreamingQueryListener 를 이용한 모니터링
		spark.streams().addListener(new SimpleListener());

		obj.run(spark);
	}

	private void run(SparkSession spark) throws Exception {

		Dataset<Row> df = spark.readStream() // streaming 모드일때는 readStream, batch 모드일때는 read
				.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test1,test2")
				.option("startingOffsets", "earliest").load();

		StreamingQuery query = df.selectExpr("cast(value as STRING)").writeStream().format("console").start();

		// StreamingQuery를 이용한 모니터링
		// monitor(query);

		query.awaitTermination();
		query.stop();
	}

	private void monitor(StreamingQuery query) throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					System.out.println("query.lastProgress:" + query.lastProgress());
					System.out.print("query.explain(true)");
					query.explain(true);
					try {
						Thread.sleep(5 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}
}
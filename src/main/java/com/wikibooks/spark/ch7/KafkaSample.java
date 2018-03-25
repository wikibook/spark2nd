package com.wikibooks.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class KafkaSample {

	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder().appName("ForeachSample").master("local[*]").getOrCreate();

		KafkaSample obj = new KafkaSample();

		Dataset<Row> df = obj.load(spark);
		df = obj.process(df);
		StreamingQuery query = obj.writeToKafkaStreamingMode(df);
	}

	private Dataset<Row> load(SparkSession spark) {

		Dataset<Row> df = spark.readStream() // streaming 모드일때는 readStream, batch 모드일때는 read
				.format("kafka")

				// 브로커 서버 정보, 필수
				.option("kafka.bootstrap.servers", "localhost:9092")

				// 토픽 지정, 필수 (assign, subscribe, subscribePattern 중 하나만 선택하여 사용)
				.option("subscribe", "test1,test2")
				// .option("assign", """{ "test1":[0,1]}, "test2":[0,1]}""")
				// .option("subscribePattern", "tes*")

				// 패치 시작 오프셋 지정, earliest나 latest
				// 또는 {topic1:{partition:offset, partition:offset...}, topic2:{...}} 형태로 지정 가능
				// 파티션별 옵셋을 지정할 경우 -2는 earliest, -1은 latest를 의미
				// streaming/batch 모드 사용 가능하며 batch 모드일 경우 latest 는 사용 불가
				// 기본값은 streaming 모드에서는 latest, batch 모드에서는 earliest
				.option("startingOffsets", "earliest")

				// 패치 종료 오프셋 지정, earliest 또는 {topic1:{partition:offset, partition:offset...},
				// topic2:{...}} 형태로 지정 가능
				// 파티션별 옵셋을 지정할 경우 -1은 latest를 의미
				// batch 모드에서만 사용 가능하며 기본값은 latest
				// .option("endingOffsets", "latest")

				// 브로커서버 장애등으로 데이터 유실이 예상될 경우 배치 작업을 실패처리 할 것인지 여부
				// streaming 모드에서만 사용 가능하며 기본값은 true (단, 데이터유실 가능성만 판단하므로 실제 데이터 유실은 발생하지 않을 수
				// 있음)
				.option("failOnDataLoss", "false")

				// polling 타임아웃. streaming과 batch 모두 모두 사용 가능하며 기본값은 512ms
				.option("kafkaConsumer.pollTimeoutMs", 512)

				// 패치에 실패할 경우 몇 번을 더 재시도 해 볼것인지에 대한 설정. streaming, batch 모드 모두 사용 가능하며 기본값은 3회
				.option("fetchOffset.numRetries", 3)

				// fetchOffset.numRetries 옵션에 따른 재시도 수행 시 시간 간격. streaming, batch 모드 모두 사용 가능하며
				// 기본값은 10ms
				.option("fetchOffset.retryIntervalMs", 10)

				// 한번에 가져올 오프셋의 크기. 토픽에 대해 지정하며 파티션이 여러개일 경우 각 파티션 별로 적절히 할당됨.
				// streaming, batch 모드 모두 사용 가능하며 기본값은 없음.
				.option("maxOffsetsPerTrigger", 20)

				// 데이터프레임 생성!
				.load();

		return df;
	}

	private Dataset<Row> process(Dataset<Row> df) {
		return df.select(df.col("value").cast("string").as("value"));
	}

	private StreamingQuery writeToConsole(Dataset<Row> df) {
		return df.writeStream().format("console").trigger(Trigger.Continuous(10)).start();
	}

	private StreamingQuery writeToKafkaStreamingMode(Dataset<Row> df) {
		return df.writeStream() // streaming 모드일때는 writeStream, batch 모드일때는 write
				.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "topic3").start();
	}

	private void writeToKafkaBatchMode(Dataset<Row> df) {
		df.write() // batch 모드일때는 write
				.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "topic3").save();
	}
}
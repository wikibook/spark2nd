from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 7.4.2절
spark = SparkSession\
          .builder\
          .appName("wordcount")\
          .master("local[*]")\
          .getOrCreate()

# port번호는 ncat 서버와 동일하게 수정해야 함
# includeTimestamp를 True로 설정한 뒤 시간값이 비정상적으로 조회될 경우
# 타임스탬프를 from_unixtime(unix_timestamp(col("timestamp"))*1000)과 같이 보정하여 사용  
lines = spark\
    .readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .option("includeTimestamp", False)\
    .load()\
    .select(col("value").alias("words"), current_timestamp().alias("ts"))

words = lines.select(explode(split(col("words"), " ")).alias("word"), window(col("ts"), "10 minute", "5 minute").alias("time"));
wordCount = words.groupBy("time", "word").count()

query = wordCount.writeStream\
      .outputMode("complete")\
      .option("truncate", False)\
      .format("console")\
      .start();

query.awaitTermination()

 

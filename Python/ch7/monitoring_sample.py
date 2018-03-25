from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from threading import Thread
import time

spark = SparkSession \
    .builder \
    .appName("kafkasample") \
    .master("local[*]") \
    .getOrCreate()
    
# 파이썬의 경우 StreamingQueryListener 를 사용할 수 없음.    

df = spark \
 .readStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "localhost:9092") \
 .option("subscribe", "test1,test2") \
 .load()

query = df.writeStream \
.format("console") \
.start()

def mon(query):
    while True:
        print(query.lastProgress)
        query.explain(True)
        time.sleep(1)

thmon = Thread(target=mon, args=(query,)) 
thmon.start()

query.awaitTermination()

'''
## 파이썬의 경우 IDE가 아닌 pyspark에서 실행

ex) $ ./bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0

'''
import os 
kafka = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
spark = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0"

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages {},{} pyspark-shell".format(kafka, spark))

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sample"

sc = SparkContext('local')
spark = SparkSession(sc)

logs = spark.read.format("kafka").option("kafka.bootstrap.servers",KAFKA_BROKER).option("subscribe",KAFKA_TOPIC).option("startingOffsets","earliest").load()

logs_df = logs.selectExpr("CAST(value as string)")
logs_value = logs.select("value").collect()
spark.parallelize(List(logs_value))

words = (" ".join(logs_value)).flatmap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word,1))
wordcount = pairs.reduceByKey(lambda x,y: x + y)
wordcount.pprint()
# message.show(20,False)
from pyspark.sql import SparkSession
import time

# Wait kafka and spark to start
time.sleep(60)

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1'
]

spark = SparkSession.builder\
    .master("spark://my-spark-master:7077")\
    .appName("kafka-example")\
    .config("spark.jars.packages", ",".join(packages))\
    .getOrCreate()


print(spark)

topic = 'testing'

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-kafka:9092") \
    .option("subscribe", topic) \
    .load()

df.selectExpr("topic", "partition", "CAST(key AS STRING)", "CAST(value AS STRING)")\
    .writeStream \
    .format('console') \
    .start().awaitTermination()

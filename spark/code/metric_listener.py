from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Wait kafka and spark to start
time.sleep(15)

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1'
]

spark = SparkSession.builder\
    .master("spark://my-spark-master:7077")\
    .appName("kafka-metric-spark")\
    .config("spark.jars.packages", ",".join(packages))\
    .getOrCreate()


print(spark)

topic = 'metrics'


kafkaDF = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "my-kafka:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .selectExpr("CAST(value AS STRING)") 


schema = StructType([ \
    StructField("timestamp", TimestampType(), True), \
    StructField("microservice_id", StringType(), True), \
    StructField("operation_type", StringType(), True), \
    StructField("action_id", StringType(), True), \
    StructField("user_id", StringType(), True), \
    StructField("value", IntegerType(), True) \
  ])
# TODO: may be add checkpoint
query = kafkaDF.select(from_json(col("value"), schema).alias("t")) \
            .select("t.timestamp", "t.microservice_id", "t.operation_type", "t.action_id", "t.user_id", "t.value") \
            .writeStream.format("console") \
            .option("truncate", "false") \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start() \
            .awaitTermination()
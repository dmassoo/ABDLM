from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql
from cassandra.cluster import Cluster
import time

# Wait kafka and spark to start
time.sleep(15)


# Attach to cassandra
cluster = Cluster(['my-cassandra'], port=9042)
session = cluster.connect()

session.execute("create keyspace IF NOT EXISTS metrics with replication = {'class' : 'SimpleStrategy', 'replication_factor':1}")
session.execute("use metrics")

session.execute("""
DROP TABLE IF EXISTS metrics.data_table""")

session.execute("""
CREATE TABLE metrics.data_table (
 timestamp timestamp,
 microservice_id text,
 operation_type text,
 action_id text,
 user_id text,
 value int,
 PRIMARY KEY(action_id)
);""")


scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1',
    f'com.datastax.spark:spark-cassandra-connector_{scala_version}:{spark_version}'
]

spark = SparkSession.builder\
    .master("spark://my-spark-master:7077")\
    .appName("kafka-metric-spark")\
    .config("spark.jars.packages", ",".join(packages))\
    .config('spark.cassandra.connection.host', ','.join(['my-cassandra:9042']))\
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
# query = kafkaDF.select(from_json(col("value"), schema).alias("t")) \
#             .select("t.timestamp", "t.microservice_id", "t.operation_type", "t.action_id", "t.user_id", "t.value") \
#             .writeStream.format("console") \
#             .option("truncate", "false") \
#             .outputMode("append") \
#             .trigger(processingTime='5 seconds') \
#             .start() \
#             .awaitTermination()

query = kafkaDF.select(from_json(col("value"), schema).alias("t")) \
            .select("t.timestamp", "t.microservice_id", "t.operation_type", "t.action_id", "t.user_id", "t.value")\
            .writeStream\
            .option("checkpointLocation", '/code/checkpoints/')\
            .format("org.apache.spark.sql.cassandra")\
            .option("keyspace", "metrics")\
            .option("table", "data_table")\
            .trigger(processingTime='10 seconds') \
            .start()\
            .awaitTermination()

print("OCHKO")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
import time
import abdlm_cassandra_configs as ccfg
from pyspark.sql.types import *

# Wait kafka and spark to start
time.sleep(15)

# Attach to cassandra
cluster = Cluster(['my-cassandra'], port=9042)
session = cluster.connect()

# Initialize keyspace abd table
session.execute(f"create keyspace IF NOT EXISTS {ccfg.keyspace} with replication = "
                "{'class' : 'SimpleStrategy', 'replication_factor':1}")
session.execute(f"use {ccfg.keyspace}")

topic = 'metrics'

session.execute("""
CREATE TABLE IF NOT EXISTS metrics_ddos ( 
 timestamp timestamp,
 action_id text,
 user_id text,
 count int,
 dummy_id text,
 PRIMARY KEY(user_id, timestamp, count)
) WITH CLUSTERING ORDER BY (timestamp DESC, count DESC);""")

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
    'org.apache.kafka:kafka-clients:3.3.1',
    'com.datastax.spark:spark-cassandra-connector_connector_2.12:3.1.0'
]

spark = SparkSession.builder \
    .master("spark://my-spark-master:7077") \
    .appName("DDOS Application") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config('spark.cassandra.connection.host', ','.join(ccfg.cassandra_nodes)) \
    .getOrCreate()

print(spark)

kafkaDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-kafka:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("action_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("value", IntegerType(), True)
])

query = kafkaDF.select(from_json(col("value"), schema).alias("t")) \
            .select("t.timestamp", "t.action_id", "t.user_id") \
            .withWatermark("timestamp", "5 seconds") \
            .groupBy("user_id", "action_id", window("timestamp", "5 seconds")) \
            .count() \
            .select("window.start", "action_id", "user_id", "count")\
            .withColumnRenamed("start", "timestamp")\
            .withColumn("dummy_id", lit("1"))\
            .writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", ccfg.keyspace)\
            .option("table", 'metrics_ddos') \
            .option("checkpointLocation", '/opt/checkpoints/ddos/') \
            .start() \
            .awaitTermination()

print("LOGS FINISH")

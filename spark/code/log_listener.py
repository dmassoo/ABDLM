from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
import time
import abdlm_cassandra_configs as ccfg

# Wait kafka and spark to start
time.sleep(15)

# Attach to cassandra
cluster = Cluster(['my-cassandra'], port=9042)
session = cluster.connect()

# Initialize keyspace abd table
session.execute(f"create keyspace IF NOT EXISTS {ccfg.keyspace} with replication = "
                "{'class' : 'SimpleStrategy', 'replication_factor':1}")
session.execute(f"use {ccfg.keyspace}")

topic = 'logs'

session.execute("""
CREATE TABLE IF NOT EXISTS logs (
 id text,
 timestamp timestamp,
 level text,
 microservice_id text,
 operation_type text,
 action_id text,
 user_id text,
 value int,
 PRIMARY KEY(action_id, id)
);""")

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
    'org.apache.kafka:kafka-clients:3.3.1',
    'com.datastax.spark:spark-cassandra-connector_connector_2.12:3.1.0'
]

spark = SparkSession.builder \
    .master("spark://my-spark-master:7077") \
    .appName("Logs App") \
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

query = kafkaDF.select(from_json(col("value"), ccfg.logsSchema).alias("t")) \
    .select("t.id", "t.timestamp", "t.level", "t.microservice_id", "t.operation_type", "t.action_id", "t.user_id") \
    .writeStream \
    .option("checkpointLocation", '/code/checkpoints/log/') \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", ccfg.keyspace) \
    .option("table", topic) \
    .trigger(processingTime='10 seconds') \
    .start() \
    .awaitTermination()

print("LOGS FINISH")

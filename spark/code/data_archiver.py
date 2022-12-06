from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from cassandra.cluster import Cluster
import time
import abdlm_cassandra_configs as ccfg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from minio import Minio

scala_version = '2.12'
spark_version = '3.3.1'
packages = ['com.datastax.spark:spark-cassandra-connector_connector_2.12:3.1.0']

spark = SparkSession.builder \
    .master("spark://my-spark-master:7077") \
    .appName("Logs Archiver") \
    .config("spark.hadoop.fs.s3a.endpoint", "nginx:9090") \
    .config("spark.hadoop.fs.s3a.access.key", ccfg.minio_key) \
    .config("spark.hadoop.fs.s3a.secret.key", ccfg.minio_secret) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config('spark.cassandra.connection.host', ','.join(ccfg.cassandra_nodes)) \
    .getOrCreate()

print(spark)
print("LOGGGG")
print(spark.read.orc('s3a://abdlm/example.orc').collect())

path = "s3a://user-jitsejan/mario-colors-two/"
rdd = spark.parallelize([('Mario', 'Red'), ('Luigi', 'Green'), ('Princess', 'Pink')])
rdd.toDF(['name', 'color']).write.csv(path)

logs_table = 'logs'
metrics_table = 'metrics'
day_month_ago = (datetime.today() - relativedelta(months=1)).date()

cassandra_logs = spark.read.format("org.apache.spark.sql.cassandra") \
    .option("keyspace", ccfg.keyspace) \
    .option("table", logs_table) \
    .load() \
    .withColumn("event_date", to_date("timestamp")) \
    .filter(col("event_date") == day_month_ago) \
    .drop(col("event_date"))

cassandra_logs.printSchema()
print("===================")
cassandra_logs.show(n=10, truncate=False)

sourceBucket = "abdlm"

outputPath = f"s3a://{sourceBucket}/logs_{day_month_ago}.orc"
cassandra_logs.write.mode("overwrite").orc(outputPath)

cassandra_metrics = spark.read.format("org.apache.spark.sql.cassandra") \
    .option("keyspace", ccfg.keyspace) \
    .option("table", metrics_table) \
    .load() \
    .withColumn("event_date", to_date("timestamp")) \
    .filter(col("event_date") == day_month_ago) \
    .drop(col("event_date"))

outputPath = f"s3a://{sourceBucket}/metrics_{day_month_ago}.orc"
cassandra_logs.write.mode("overwrite").orc(outputPath)

spark.stop()

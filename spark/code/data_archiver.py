from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import abdlm_cassandra_configs as ccfg
from datetime import datetime
from dateutil.relativedelta import relativedelta

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    'com.datastax.spark:spark-cassandra-connector_connector_2.12:3.1.0',
    'org.apache.spark:spark-hadoop-cloud_2.13:3.2.0'
]

object_storage_url = "http://172.22.0.2:9000"

spark = SparkSession.builder \
    .master("spark://my-spark-master:7077") \
    .appName("Logs Archiver") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config('spark.cassandra.connection.host', ','.join(ccfg.cassandra_nodes)) \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.parquet.enable.summary-metadata", False) \
    .config("spark.sql.orc.splits.include.file.footer", True) \
    .config("spark.sql.parquet.mergeSchema", False) \
    .config("spark.sql.hive.metastorePartitionPruning", True) \
    .getOrCreate()
spark.sparkContext._jsc \
    .hadoopConfiguration().set("fs.s3a.access.key", ccfg.minio_key)
spark.sparkContext._jsc \
    .hadoopConfiguration().set("fs.s3a.secret.key", ccfg.minio_secret)

spark.sparkContext._jsc \
    .hadoopConfiguration().set("fs.s3a.endpoint", object_storage_url)
spark.sparkContext._jsc \
    .hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.path.style.access", "true"
)
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.connection.ssl.enabled", "true"
)

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

bucket = "abdlm"

outputPath = f"s3a://{bucket}/logs_{day_month_ago}.parquet"
cassandra_logs.write.mode("overwrite").csv(outputPath)

cassandra_metrics = spark.read.format("org.apache.spark.sql.cassandra") \
    .option("keyspace", ccfg.keyspace) \
    .option("table", metrics_table) \
    .load() \
    .withColumn("event_date", to_date("timestamp")) \
    .filter(col("event_date") == day_month_ago) \
    .drop(col("event_date"))

outputPath = f"s3a://{bucket}/metrics_{day_month_ago}.parquet"
cassandra_metrics.write.mode("overwrite").csv(outputPath)

spark.stop()

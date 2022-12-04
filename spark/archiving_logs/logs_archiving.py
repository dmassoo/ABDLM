from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
import time
import spark.code.abdlm_cassandra_configs as ccfg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from minio import Minio

# Wait kafka and spark to start
time.sleep(15)

# Attach to cassandra
cluster = Cluster(['my-cassandra'], port=9042)
session = cluster.connect()

# Initialize keyspace abd table


scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
    'org.apache.kafka:kafka-clients:3.3.1',
    'com.datastax.spark:spark-cassandra-connector_connector_2.12:3.1.0'
]

spark = SparkSession.builder \
    .master("spark://my-spark-master:7077") \
    .appName("Logs Archiver") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config('spark.cassandra.connection.host', ','.join(ccfg.cassandra_nodes)) \
    .getOrCreate()

print(spark)

logs_table = 'logs'
day_month_ago = (datetime.today() - relativedelta(months=1)).date()

cassandra_logs = spark.read.format("org.apache.spark.sql.cassandra") \
    .option("keyspace", ccfg.keyspace) \
    .option("table", logs_table) \
    .filter(col("timestamp").date() == day_month_ago) \
    .load

cassandra_logs.printSchema()
print("===================")
cassandra_logs.show(False)

filePath = '/tmp/logs_archived.orc'
cassandra_logs.to_orc('%s' % filePath, mode='overwrite')



bucket_name = "abdlm"

# todo: add volumes to minio and service node port
client = Minio(
    "localhost:9000",
    access_key="IGcZVwPLC88N9IMJ",
    secret_key="YW6qMvNGKFNIC4hKsMQDnsVVD9hu1zF6",
    secure=False
)

found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
else:
    print("Bucket 'abdlm' already exists")

client.fput_object(bucket_name, file_path=filePath, object_name=f"logs_{day_month_ago}.ork")

print("LOGS ARCHIVING IS FINISHED")

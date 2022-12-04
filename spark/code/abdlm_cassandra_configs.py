from pyspark.sql.types import *


keyspace = 'abdlm'
cassandra_nodes = ['my-cassandra:9042']


metricsSchema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), True),
    StructField("microservice_id", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("action_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("value", IntegerType(), True)
])

logsSchema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), True),
    StructField("level", StringType(), True),
    StructField("microservice_id", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("action_id", StringType(), True),
    StructField("user_id", StringType(), True)
])
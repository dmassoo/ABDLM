import os
from pyspark.sql.types import *


keyspace = 'abdlm'
cassandra_nodes = os.environ.get("CASSANDRA_NODES").split(",")
cassandra_port = os.environ.get("CASSANDRA_PORT")
cassandra_user = os.environ.get("CASSANDRA_USER")
cassandra_password = os.environ.get("CASSANDRA_PASSWORD")
kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def getCassandraCredential():
    return {'username': cassandra_user, 'password': cassandra_password}


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
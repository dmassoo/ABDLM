/opt/bitnami/spark/bin/spark-submit --master spark://my-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0   --deploy-mode client  --driver-memory 1g  --executor-memory 1g  --executor-cores 1 --conf spark.standalone.submit.waitAppCompletion=False --conf spark.cores.max=1  /code/views_transactions_agg.py
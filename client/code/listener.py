import kafka
# from kafka import KafkaConsumer
import time
from cassandra.cluster import Cluster


# Define server with port
bootstrap_servers = ['my-kafka:9092']

# Define topic name from where the message will recieve
topic_name = 'testing'




# Wait kafka and cassandra to start
time.sleep(45)

# Attach to cassandra
cluster = Cluster(['my-cassandra'], port=9042)
session = cluster.connect()

session.execute("create keyspace IF NOT EXISTS testing_ks with replication = {'class' : 'SimpleStrategy', 'replication_factor':1}")
session.execute("use testing_ks")

session.execute("""
DROP TABLE IF EXISTS testing_ks.some_table""")

session.execute("""
CREATE TABLE testing_ks.some_table (
 id int,
 topic text,
 value text,
 PRIMARY KEY(id)
);""")

# Initialize consumer variable
consumer = kafka.KafkaConsumer(topic_name, group_id='my-group', bootstrap_servers = bootstrap_servers)

# Read and print message from consumer
for msg in consumer:
    print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
    # Add some staff to cassandra table
    session.execute(f"INSERT INTO testing_ks.some_table (id, topic, value) VALUES ({int(msg.value)},'{msg.topic}','{int(msg.value)}')")
    # Print all the rows
    row = session.execute("""select * from testing_ks.some_table""")[-1]
    print(row)


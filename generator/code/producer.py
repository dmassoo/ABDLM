import kafka
# from kafka import KafkaProducer
import time
from tqdm import tqdm
import json
from parallel_data_generator import metrics_logs_generator
import itertools

# TODO: work with logs printing
def serialize(data):
    for r in data:
        for k in r:
            yield (json.dumps(k), "utf-8")

bootstrap_servers = ['my-kafka:9092']

def attach_producer() -> kafka.KafkaProducer:
    try:
        producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=serialize)
        print("Success: Attached to kafka broker")
        return producer
    except kafka.errors.NoBrokersAvailable:
        print("Failure: Kafka broker is anavailable")
        return None

max_attempts = 5
retry_time = 15

print("Trying to attach producer")
for attempt in range(max_attempts):
    producer = attach_producer()
    # Found a kafka broker
    if producer != None:
        break
    # No kafka broker found
    if attempt == max_attempts - 1:
        print("No kafka broker found. Exit")
        exit(1)

    time.sleep(retry_time)
    print("Retry attaching")


if producer == None:
    print("Sasat")
    exit(1)

iterations = 1000
topic_metrics = 'metrics'
topic_logs = 'logs'
time_wait = 1

while True:
    metrics = metrics_logs_generator()[0]
    logs = metrics_logs_generator()[1]
    # print("sending value = " + value)
    producer.send(topic=topic_metrics, value=metrics)
    producer.send(topic=topic_logs, value=logs)
    time.sleep(time_wait)



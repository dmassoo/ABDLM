import kafka
# from kafka import KafkaProducer
import time
from tqdm import tqdm
import json
from parallel_data_generator import metrics_logs_generator, resources, MICROSERVICE_ID
import itertools

bootstrap_servers = ['my-kafka:9092']


def attach_producer() -> kafka.KafkaProducer:
    try:
        producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                       value_serializer=lambda item: json.dumps(item).encode('utf8'))
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
topic_resources = 'resources'
time_wait = 1

while True:
    metrics_logs = metrics_logs_generator()
    metrics = list(itertools.chain(*metrics_logs[0]))
    logs = list(itertools.chain(*metrics_logs[1]))
    resourse = resources()
    # for i in tqdm(range(len(metrics))):
        # print("sending value = " + value)
        # producer.send(topic=topic_metrics, value=metrics[i])
        # producer.send(topic=topic_logs, value=logs[i])
    for indx, id in enumerate(MICROSERVICE_ID):
        print("sending resourse: ", resourse[indx])
        producer.send(topic=topic_resources, value=resourse[indx])

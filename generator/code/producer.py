import kafka
# from kafka import KafkaProducer
import time

# TODO: work with logs printing

bootstrap_servers = ['my-kafka:9092']

def attach_producer() -> kafka.KafkaProducer:
    try:
        producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=str.encode)
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
topic_name = 'testing'
time_wait = 1

for i in range(iterations):
    value = str(i)
    print("sending value = " + value)
    producer.send(topic=topic_name, value=value)
    time.sleep(time_wait)




import random
import string
import uuid
from datetime import datetime, timezone
from json import dumps
from numpy.random import choice
from kafka import KafkaProducer

LEVEL = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR']
MICROSERVICE_ID = ['VIEW', 'BUY', 'CANCEL', 'REFUND']
OPERATION_TYPE = ['VIEW', 'BUY', 'CANCEL', 'REFUND']


def generate_log_entry():
    log_entry = {}

    dt = datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    log_entry['timestamp'] = utc_timestamp
    log_entry['level'] = choice(LEVEL)
    log_entry['microservice_id'] = choice(MICROSERVICE_ID)
    log_entry['operation_type'] = choice(OPERATION_TYPE)
    log_entry['operation_id'] = str(uuid.uuid4())
    log_entry['user_id'] = str(uuid.uuid4())
    log_entry['message'] = ''.join(random.choice(string.ascii_letters) for _ in range(60))

    return log_entry


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         api_version=(0, 11, 5),
                         key_serializer=str.encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
my_topic = 'logs'


def simulate_logs():
    data = generate_log_entry()

    try:
        print("try")
        future = producer.send(topic=my_topic, key=data['level'], value=data)
        record_metadata = future.get(timeout=10)

        print('--> The message has been sent to a topic: \
                {}, partition: {}, offset: {}'
              .format(record_metadata.topic,
                      record_metadata.partition,
                      record_metadata.offset))

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))

    finally:
        producer.flush()


def simulate_logs2():
    print("simulate_logs2")
    data = generate_log_entry()
    producer.send(topic=my_topic, key=data['level'], value=data)


while True:
    simulate_logs2()
# if __name__ == "__main__":
#     while True:
#         simulate_logs()

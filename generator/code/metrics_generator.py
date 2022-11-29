import uuid
from datetime import datetime, timezone
from json import dumps

from kafka import KafkaProducer
from numpy.random import choice

EVENT_TYPE = ['VIEW', 'TRANSACTION', 'CPU', 'RAM']
MICROSERVICE_ID = ['VIEW', 'BUY', 'CANCEL', 'REFUND']


def generate_metrics():
    metrics = {}

    dt = datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    metrics['timestamp'] = utc_timestamp

    metrics['microservice_id'] = choice(MICROSERVICE_ID)
    metrics['operation_id'] = str(uuid.uuid4())
    metrics['user_id'] = str(uuid.uuid4())
    metrics['event_type'] = choice(EVENT_TYPE)

    def generate_metrics_value():
        return 0

    metrics['value'] = generate_metrics_value()

    return metrics


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: x,
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
my_topic = 'metrics'


def simulate_metrics():
    data = generate_metrics()

    try:
        future = producer.send(topic=my_topic, key=data['event_type'], value=data)
        record_metadata = future.get(timeout=10)

        print('--> The message has been sent to a topic: \
                {}, partition: {}, offset: {}'
              .format(record_metadata.topic,
                      record_metadata.partition,
                      record_metadata.offset))

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))


if __name__ == "__main__":
    while True:
        simulate_metrics()

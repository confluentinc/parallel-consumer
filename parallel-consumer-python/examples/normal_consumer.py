#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from examples.kafka import get_consumer, create_topic
from pyallel_consumer import load_config

if __name__ == '__main__':
    kafka_config_path = '~/.confluent/java.config'
    props = load_config(kafka_config_path)

    topic = 'pyallel'
    create_topic(topic, props)
    consumer = get_consumer(props)
    consumer.subscribe([topic])
    try:
        while True:
            records = consumer.poll(100)
            for record in records:
                value = record.value()
                print(f'Consumed record with key {record.key()} and value {value}')
    finally:
        consumer.close()

#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from examples.kafka import get_consumer, create_topic, get_producer
from pyallel_consumer import load_config

if __name__ == '__main__':
    from jio.confluent.parallelconsumer import ParallelConsumerOptions, ParallelStreamProcessor

    kafka_config_path = '~/.confluent/java.config'
    props = load_config(kafka_config_path)

    topic = 'pyallel'
    create_topic(topic, props)

    consumer = get_consumer(props)
    producer = get_producer(props)

    options = ParallelConsumerOptions.builder() \
        .ordering(ParallelConsumerOptions.ProcessingOrder.KEY) \
        .maxConcurrency(1000) \
        .consumer(consumer) \
        .producer(producer) \
        .build()
    processor = ParallelStreamProcessor.createEosStreamProcessor(options)
    processor.subscribe([topic])
    processor.poll(lambda o: print(f"Concurrently processing a record: {o}"))

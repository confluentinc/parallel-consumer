#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from busypie import wait, MINUTE
from jpype import JProxy

from pyallel_consumer import start_jvm
from tests import timeit, sleep_a_bit

start_jvm()

from java.util.concurrent import Callable
from java.util.concurrent.atomic import AtomicInteger
from java.util.concurrent.Executors import newCachedThreadPool

from org.apache.kafka.clients.producer import KafkaProducer
from org.apache.kafka.clients.consumer import KafkaConsumer

from jio.confluent.parallelconsumer import ParallelConsumerOptions, ParallelStreamProcessor

MAX_TIMEOUT = (15, MINUTE)


@timeit
def consume_messages(consumer: KafkaConsumer, num_messages: int):
    count = AtomicInteger(0)
    all_records = []

    def process_messages():
        nonlocal count
        while count.get() < num_messages:
            records = list(consumer.poll(500).iterator())
            print(f'Polling batch of {len(records)} messages')
            for _ in records:
                sleep_a_bit()
            count.getAndAdd(len(records))
            all_records.extend(records)

    newCachedThreadPool().submit(JProxy(Callable, dict(call=process_messages)))
    success_criterion = lambda: count.get() == num_messages
    wait().at_most(*MAX_TIMEOUT).until(success_criterion)
    assert success_criterion()


def test_normal_kafka_consumer(loaded_topic: str, consumer: KafkaConsumer, num_messages: int):
    consumer.subscribe([loaded_topic])
    print('Starting to read back')
    consume_messages(consumer, num_messages)


@timeit
def parallel_consume_messages(processor, num_messages):
    count = AtomicInteger(0)

    def process_message(message):
        nonlocal count
        sleep_a_bit()
        count.getAndIncrement()

    processor.poll(process_message)
    success_criterion = lambda: count.get() >= num_messages
    wait().at_most(*MAX_TIMEOUT).until(success_criterion)
    assert success_criterion()


def test_async_consume_and_process(loaded_topic: str, consumer: KafkaConsumer, transactional_producer: KafkaProducer,
                                   num_messages: int):
    options = ParallelConsumerOptions.builder() \
        .ordering(ParallelConsumerOptions.ProcessingOrder.KEY) \
        .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) \
        .producer(transactional_producer) \
        .consumer(consumer) \
        .maxConcurrency(3) \
        .build()
    processor = ParallelStreamProcessor.createEosStreamProcessor(options)
    try:
        processor.subscribe([loaded_topic])
        parallel_consume_messages(processor, num_messages)
    finally:
        processor.close()

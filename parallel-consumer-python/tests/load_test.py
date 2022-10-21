#
# Copyright (C) 2020-2022 Confluent, Inc.
#

import random
import string
from pathlib import Path
from random import randint
from sys import maxsize
from time import sleep

from busypie import wait, MINUTE
from jpype.types import JInt
from jpype import JProxy
from pytest import fixture
from testcontainers.kafka import KafkaContainer

from pyallel_consumer import start_jvm
from . import timeit
from .kafka import create_topic, create_client_properties

start_jvm()

from java.util import Locale, Properties
from java.util.concurrent import Callable
from java.util.concurrent.atomic import AtomicInteger
from java.util.concurrent.Executors import newCachedThreadPool

from org.apache.kafka.common import IsolationLevel
from org.apache.kafka.common.serialization import StringSerializer, StringDeserializer
from org.apache.kafka.clients.producer import ProducerConfig, KafkaProducer, ProducerRecord
from org.apache.kafka.clients.consumer import ConsumerConfig, KafkaConsumer, OffsetResetStrategy

from jio.confluent.parallelconsumer import ParallelConsumerOptions, ParallelStreamProcessor


MAX_TIMEOUT = (1, MINUTE)


@fixture(scope='module')
def kafka() -> KafkaContainer:
    kafka_container = KafkaContainer("confluentinc/cp-kafka:7.2.2") \
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") \
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") \
        .with_env("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") \
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") \
        .with_env("KAFKA_transaction_state_log_replication_factor", "1") \
        .with_env("KAFKA_transaction_state_log_min_isr", "1")
    kafka_container.start()
    yield kafka_container
    kafka_container.stop()


@fixture
def common_props(kafka) -> Properties:
    return create_client_properties({'bootstrap.servers': kafka.get_bootstrap_server()})


@fixture
def producer_props(common_props) -> Properties:
    producer_props = common_props
    producer_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.__name__)
    producer_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.__name__)
    return producer_props


@fixture
def producer(producer_props) -> KafkaProducer:
    return KafkaProducer(producer_props)


@fixture
def transactional_producer(producer_props) -> KafkaProducer:
    config_name = f'{transactional_producer.__name__}:{randint(0, maxsize)}'
    producer_props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, config_name)
    producer_props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, JInt(10000))
    return KafkaProducer(producer_props)


@fixture
def consumer(common_props) -> KafkaConsumer:
    group_id = f'group-1-{randint(0, maxsize)}'
    consumer_props = common_props
    consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, group_id)
    consumer_props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, False)
    consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.__name__)
    consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.__name__)
    consumer_props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                       IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT))
    reset_strategy = OffsetResetStrategy.EARLIEST.name().toLowerCase()
    consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset_strategy);
    consumer_props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, JInt(10_000));
    return KafkaConsumer(consumer_props)


@fixture
def num_messages() -> int:
    return 4_000


@fixture
def loaded_topic(kafka, common_props, producer, num_messages) -> str:
    topic_name = f'{Path(__file__).stem}-{randint(0, maxsize)}'
    create_topic(topic_name, common_props, num_partitions=1)

    future_metadata_results = publish_messages(producer, topic_name, num_messages, num_keys=num_messages / 100)
    used_partitions = {metadata.get().partition() for metadata in future_metadata_results}
    print(f'Used {len(used_partitions)} partitions')
    return topic_name


@timeit
def publish_messages(producer: KafkaProducer, topic_name: str, num_messages: int, num_keys: int):
    print('Start publishing...')
    future_metadata_results = []
    for _ in range(num_messages):
        key = str(randint(0, num_keys))
        message_size_bytes = 500
        value = ''.join(random.choices(string.ascii_uppercase + string.digits, k=message_size_bytes))
        future_metadata = producer.send(ProducerRecord(topic_name, key, value))
        future_metadata_results.append(future_metadata)
    return future_metadata_results


def sleep_a_bit():
    sleep(randint(0, 5) / 1000)


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
    processor.close()
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
    processor.subscribe([loaded_topic])
    parallel_consume_messages(processor, num_messages)

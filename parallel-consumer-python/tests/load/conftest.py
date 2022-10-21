#
# Copyright (C) 2020-2022 Confluent, Inc.
#

import random
import string
from copy import deepcopy
from pathlib import Path
from random import randint
from sys import maxsize
from typing import Dict

from jpype.types import JInt
from pytest import fixture
from testcontainers.kafka import KafkaContainer

from pyallel_consumer import start_jvm
from tests import timeit
from tests.kafka import create_topic, create_client_properties

start_jvm()

from java.util import Locale

from org.apache.kafka.common import IsolationLevel
from org.apache.kafka.common.serialization import StringSerializer, StringDeserializer
from org.apache.kafka.clients.producer import ProducerConfig, KafkaProducer, ProducerRecord
from org.apache.kafka.clients.consumer import ConsumerConfig, KafkaConsumer, OffsetResetStrategy


@fixture
def common_props(kafka_container) -> Dict:
    return {'bootstrap.servers': kafka_container.get_bootstrap_server()}


@fixture
def producer_props(common_props) -> Dict:
    producer_props = deepcopy(common_props)
    producer_props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer.__name__
    producer_props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer.__name__
    return producer_props


@fixture
def producer(producer_props) -> KafkaProducer:
    producer = KafkaProducer(create_client_properties(producer_props))
    yield producer
    producer.close()


@fixture
def transactional_producer(producer_props: Dict) -> KafkaProducer:
    config_name = f'{transactional_producer.__name__}:{randint(0, maxsize)}'
    producer_props[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = config_name
    producer_props[ProducerConfig.TRANSACTION_TIMEOUT_CONFIG] = JInt(10000)
    producer = KafkaProducer(create_client_properties(producer_props))
    yield producer
    producer.close()


@fixture
def consumer_props(common_props: Dict) -> Dict:
    consumer_props = deepcopy(common_props)
    group_id = f'group-1-{randint(0, maxsize)}'
    consumer_props[ConsumerConfig.GROUP_ID_CONFIG] = group_id
    consumer_props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = False
    consumer_props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer.__name__
    consumer_props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer.__name__
    consumer_props[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = \
        IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT)
    reset_strategy = OffsetResetStrategy.EARLIEST.name().toLowerCase()
    consumer_props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = reset_strategy
    consumer_props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = JInt(10_000)
    return consumer_props


@fixture
def consumer(consumer_props: Dict) -> KafkaConsumer:
    consumer = KafkaConsumer(create_client_properties(consumer_props))
    yield consumer
    consumer.close()


@fixture
def num_messages() -> int:
    return 4_000


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


@fixture
def loaded_topic(kafka_container, common_props, producer, num_messages) -> str:
    topic_name = f'{Path(__file__).stem}-{randint(0, maxsize)}'
    create_topic(topic_name, common_props, num_partitions=1)

    future_metadata_results = publish_messages(producer, topic_name, num_messages, num_keys=num_messages / 100)
    used_partitions = {metadata.get().partition() for metadata in future_metadata_results}
    print(f'Used {len(used_partitions)} partitions')
    return topic_name


@fixture(scope='module')
def kafka_container() -> KafkaContainer:
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

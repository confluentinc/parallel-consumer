#
# Copyright (C) 2020-2022 Confluent, Inc.
#
from copy import deepcopy
from dataclasses import dataclass
from random import randint
from sys import maxsize
from typing import Dict

from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer
from pytest import fixture
from functools import partial

from tests import timeit


@dataclass
class Result:
    key: str
    value: str


@timeit
def consume_messages(consumer: Consumer, num_messages: int):
    count = 0
    all_records = []
    try:
        while count < num_messages:
            records = consumer.consume(10_000, 0.5)
            if not records:
                continue
            count += len(records)
            deserializer = partial(StringDeserializer(), ctx=None)
            for record in records:
                all_records.append(Result(deserializer(record.key()), deserializer(record.value())))
            all_records.extend(records)
    finally:
        consumer.close()
    assert count == num_messages


@fixture
def python_consumer_props(common_props: Dict) -> Dict:
    python_props = deepcopy(common_props)
    group_id = f'group-1-{randint(0, maxsize)}'
    python_props['group.id'] = group_id
    python_props['enable.auto.commit'] = False
    python_props['isolation.level'] = 'read_committed'
    python_props['auto.offset.reset'] = 'earliest'
    return python_props


def test_normal_python_client(python_consumer_props, loaded_topic: str, num_messages: int):
    consumer = Consumer(python_consumer_props)
    consumer.subscribe([loaded_topic])
    print('Starting to read back')
    consume_messages(consumer, num_messages)

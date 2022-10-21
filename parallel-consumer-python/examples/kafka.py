#
# Copyright (C) 2020-2022 Confluent, Inc.
#

import jpype


def get_consumer(props: 'Properties'):
    from org.apache.kafka.common.serialization import StringDeserializer
    from org.apache.kafka.clients.consumer import ConsumerConfig, KafkaConsumer
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.__name__)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    return KafkaConsumer(props)


def get_producer(props: 'Properties'):
    from org.apache.kafka.common.serialization import StringSerializer
    from org.apache.kafka.clients.producer import ProducerConfig, KafkaProducer
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.__name__)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer")
    return KafkaProducer(props)


def create_topic(topic: str, props: 'Properties'):
    from java.util import Collections, Optional
    from org.apache.kafka.clients.admin import AdminClient
    from org.apache.kafka.clients.admin import NewTopic
    from org.apache.kafka.common.errors import TopicExistsException

    new_topic = NewTopic(topic, Optional.empty(), Optional.empty())
    try:
        with AdminClient.create(props) as admin_client:
            admin_client.createTopics(Collections.singletonList(new_topic)).all().get()
    except TopicExistsException:
        print(f'Topic {topic} already exists')
    except jpype.JException as e:
        if 'TopicExistsException' in e.args[0]:
            print(f'Topic {topic} already exists')
        else:
            raise e

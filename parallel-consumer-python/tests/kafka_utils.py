#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from typing import Dict, Union

import jpype


def create_topic(topic: str, props: Union[Dict, 'Properties'], num_partitions: int):
    from java.util import Collections, Optional
    from org.apache.kafka.clients.admin import AdminClient
    from org.apache.kafka.clients.admin import NewTopic
    from org.apache.kafka.common.errors import TopicExistsException

    if isinstance(props, dict):
        props = create_client_properties(props)
    new_topic = NewTopic(topic, num_partitions, 1)
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


def create_client_properties(props: Dict):
    from java.util import Properties
    config = Properties()
    for (key, value) in props.items():
        config.put(key, value)
    return config

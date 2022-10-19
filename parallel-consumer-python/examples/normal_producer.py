from examples.kafka import create_topic, get_producer
from pyallel_consumer import load_config

if __name__ == '__main__':
    kafka_config_path = '~/.confluent/java.config'
    props = load_config(kafka_config_path)

    topic = 'pyallel'
    create_topic(topic, props)
    producer = get_producer(props)

    from org.apache.kafka.clients.producer import ProducerRecord

    for i in range(10):
        producer.send(ProducerRecord(topic, str(i), 'Hello from Python via JPype'))
    producer.flush()
    producer.close()

package io.confluent.parallelconsumer.internal;

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;

import java.util.concurrent.ExecutorService;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @author Antony Stubbs
 */
@ToString
@FieldDefaults(makeFinal = true, level = PRIVATE)
public class BrokerProducer<K, V> {

    @Getter(PROTECTED)
    Node brokerNode;

    ProduceQueue<K, V> queue;

    public BrokerProducer(Node brokerNode, KafkaProducer<K, V> producer, ExecutorService executorPool) {
        this.queue = new ProduceQueue<>(producer, executorPool);
        this.brokerNode = brokerNode;
    }

    public void send(ProducerRecord<K, V> makeRecord) {
        queue.send(makeRecord);
    }

    public int getOutboundQueueSize() {
        return queue.outboundSize();
    }

    public void supervise() {
        queue.supervise();
    }
}

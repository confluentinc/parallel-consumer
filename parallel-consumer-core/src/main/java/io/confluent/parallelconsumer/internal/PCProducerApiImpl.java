package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.PCProducerApi;
import io.confluent.parallelconsumer.ParallelConsumerException;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @author Antony Stubbs
 */
@Slf4j
@ThreadSafe
@FieldDefaults(level = PRIVATE)
public class PCProducerApiImpl<K, V> implements PCProducerApi<K, V>, Closeable {

    final Comparator<BrokerProducer<K, V>> brokerLocalQueueComparator = Comparator.comparing(BrokerProducer::getOutboundQueueSize);

    // visible for testing
    @Getter(PROTECTED)
    final
    List<BrokerProducer<K, V>> brokerProducers = new ArrayList<>();

    final AdminClient ac;

    final ExecutorService executorPool;

    int threadCount = 0;

    public PCProducerApiImpl(AdminClient ac, Supplier<KafkaProducer<K, V>> producerSupplier) throws ExecutionException, InterruptedException {
        this.ac = ac;

        final ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r);
            t.setName(ProduceQueue.class.getSimpleName() + "-worker-" + threadCount);
            threadCount++;
            return t;
        };
        executorPool = Executors.newCachedThreadPool(threadFactory);

        getBrokers().forEach(broker -> {
            var kafkaProducer = producerSupplier.get();
            brokerProducers.add(new BrokerProducer<>(broker, kafkaProducer, executorPool));
        });
    }

    private Collection<Node> getBrokers() throws ExecutionException, InterruptedException {
        var nodes = ac.describeCluster().nodes().get();
        if (nodes.isEmpty()) {
            throw new ParallelConsumerException("No brokers found");
        }
        return nodes;
    }

    @Override
    public void sendToLeastLoaded(ProducerRecord<K, V> makeRecord) {
        BrokerProducer<K, V> leastLoadedBroker = getLeastLoadedBroker();
        log.debug("Selected broker {} for sending, other broker queues were: {}", leastLoadedBroker, brokerProducers.stream()
                .filter(b -> b != leastLoadedBroker).map(BrokerProducer::getOutboundQueueSize).toArray());
        leastLoadedBroker.send(makeRecord);
    }

    public int getClusterSize() {
        return brokerProducers.size();
    }

    @VisibleForTesting
    protected BrokerProducer<K, V> getLeastLoadedBroker() {
        //noinspection OptionalGetWithoutIsPresent - we know there is at least one
        Optional<BrokerProducer<K, V>> min = brokerProducers.stream().min(brokerLocalQueueComparator);
        return min.get();
    }

    @Override
    public void close() throws IOException {
        executorPool.shutdown();
    }

    public BrokerProducer<K, V> getBroker(int brokerIndex) {
        return brokerProducers.get(brokerIndex);
    }

    public void supervise() {
        for (BrokerProducer<K, V> brokerProducer : brokerProducers) {
            brokerProducer.supervise();
        }
    }
}

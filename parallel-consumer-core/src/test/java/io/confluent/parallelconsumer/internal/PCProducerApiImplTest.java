package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.PCProducerApi;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Antony Stubbs
 * @see PCProducerApiImpl
 */
@Slf4j
class PCProducerApiImplTest {

    public static final RecordMetadata MOCK_METADATA = new RecordMetadata(null, 0, 0, 0, 0, 0);
    private final Queue<FuturePair> futures = new LinkedList<>();
    int NUMBER_OF_BROKERS = 3;

    int numberOfRecordsSent;

    PCProducerApi<String, String> producerApi;

    PCProducerApiImpl<String, String> producerApiImpl;

    PCProducerApiImplTest() throws ExecutionException, InterruptedException {
        var ac = mock(AdminClient.class);
        List<Node> nodes = UniLists.of(new Node(0, "localhost", 9092),
                new Node(1, "localhost", 9092),
                new Node(2, "localhost", 9092));
        var mock = mock(DescribeClusterResult.class);
        when(mock.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
        when(ac.describeCluster()).thenReturn(mock);
        KafkaProducer producer = mock(KafkaProducer.class);
        when(producer.send(Mockito.any())).thenAnswer(invocation -> {
            KafkaFutureImpl<RecordMetadata> kafkaFuture = new KafkaFutureImpl<>();
            futures.add(new FuturePair(invocation, kafkaFuture));
            return kafkaFuture;
        });
        producerApiImpl = new PCProducerApiImpl<String, String>(ac, () -> producer);
        producerApi = producerApiImpl;

        assertThat(producerApiImpl.getClusterSize()).isEqualTo(3);
    }


    @Test
    void testLeastLoaded() {
        assertThatLeastLoadedIs(0);

        assertThatBrokerOutboundQueueSizesAre(0, 0, 0);

        //fill up the producer's buffer to the threshold
        int recordNeededToBlock = ProduceQueue.QUEUE_THRESHOLD * NUMBER_OF_BROKERS;
        Range.range(recordNeededToBlock)
                .forEach(i -> {
                    producerApiImpl.supervise();
                    sendOne();
                });

        producerApiImpl.supervise();

        assertThatBrokerOutboundQueueSizesAre(0, 0, 0);
        assertThatLeastLoadedIs(0);

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(1, 0, 0);
        assertThatLeastLoadedIs(1);

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(1, 1, 0);
        assertThatLeastLoadedIs(2);

        // ack one
        makeBrokerAckOneRecord();
        assertThatOutboundQueueSizesAreWithinOne();

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(1, 1, 0);
        assertThatLeastLoadedIs(2);

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(1, 1, 1);
        assertThatLeastLoadedIs(0);

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(2, 1, 1);

        sendOne();
        assertThatBrokerOutboundQueueSizesAre(2, 2, 1);
        assertThatLeastLoadedIs(2);

        sendOne();
        sendOne();
        sendOne();
        assertThatBrokerOutboundQueueSizesAre(3, 3, 2);
        assertThatOutboundQueueSizesAreWithinOne();
    }

    private void assertThatOutboundQueueSizesAreWithinOne() {
        Awaitility.await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    var brokers = producerApiImpl.getBrokerProducers();
                    var min = brokers.stream().mapToInt(BrokerProducer::getOutboundQueueSize).min().getAsInt();
                    var max = brokers.stream().mapToInt(BrokerProducer::getOutboundQueueSize).max().getAsInt();
                    var range = max - min;
                    assertWithMessage("Brokers respective queue sizes are within one (with %s records sent)", numberOfRecordsSent)
                            .that(range)
                            .isAtMost(1);
                });
    }

    private void makeBrokerAckOneRecord() {
        log.debug("Making broker ack one record");
        Optional.ofNullable(futures.poll()).map(f -> f.getFuture().complete(MOCK_METADATA));
    }

    private void sendOne() {
        producerApi.sendToLeastLoaded(makeRecord());
        this.numberOfRecordsSent++;
    }

    private void assertThatBrokerOutboundQueueSizesAre(int i, int i1, int i2) {
        Awaitility.await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertWithMessage("Brokers respective queue sizes (with %s records sent)", numberOfRecordsSent)
                            .that(producerApiImpl.getBrokerProducers().stream()
                                    .map(BrokerProducer::getOutboundQueueSize).toList())
                            .containsExactly(i, i1, i2);
                });
    }

    private void assertThatLeastLoadedIs(int id) {
        assertThat(producerApiImpl.getLeastLoadedBroker().getBrokerNode().id()).isEqualTo(id);
    }

    private void assertThatBrokerHasQueueSize(int brokerIndex, int queueSize) {
        assertThat(producerApiImpl.getBroker(brokerIndex).getOutboundQueueSize()).isEqualTo(queueSize);
    }

    @Test
    void testBackPressure() {
        sendOne();
        sendOne();
        sendOne();

        Awaitility.await().until(() -> producerApiImpl.getClusterSize() == 2);
    }

    @NonNull
    private static ProducerRecord<String, String> makeRecord() {
        return new ProducerRecord<>(getTopic(), "key", "value");
    }

    @NonNull
    private static String getTopic() {
        return "topic";
    }

    @Value
    private class FuturePair {
        InvocationOnMock invocation;
        KafkaFutureImpl<RecordMetadata> future;
    }
}
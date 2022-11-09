package io.confluent.parallelconsumer.integrationTests;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.confluent.csid.utils.JavaUtils.catchAndWrap;
import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.integrationTests.OffsetCommitTest.AssignmentState.BOTH;
import static io.confluent.parallelconsumer.integrationTests.OffsetCommitTest.AssignmentState.FIRST;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @author Antony Stubbs
 */
@Slf4j
class OffsetCommitTest extends BrokerIntegrationTest {

    KafkaClientUtils kcu = getKcu();
    int numPartitions = 1;
    String topicName = getClass().getName() + "-" + System.currentTimeMillis();
    List<String> topicList = of(topicName);
    TopicPartition tp = new TopicPartition(topicName, 0);
    OffsetAndMetadata offsetZeroMeta = new OffsetAndMetadata(0);
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
    Map<TopicPartition, OffsetAndMetadata> offsetZeroMetaTp = Map.of(tp, offsetZeroMeta);
    Duration timeout = Duration.ofSeconds(1);
    int numberToSend = 5;
    KafkaConsumer<String, String> firstConsumer;
    KafkaConsumer<String, String> secondConsumer;

    @SneakyThrows
    @Test
    void canTerminateConsumerConnection() {
        kcu.getAdmin().createTopics(of(newTopic)).all().get();

//        killFirstConsumerConnection();

        var overridingOptions = new Properties();
        var brokerProxy = getBrokerProxy();
        var proxyPort = brokerProxy.getProxyPort();
        var proxiedBootstrapServers = kcu.getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        firstConsumer = kcu.createNewConsumer(topicName, overridingOptions);
        firstConsumer.subscribe(topicList);
        firstConsumer.enforceRebalance(); // todo remove


        var configResourceConfigMap = kcu.getAdmin().describeConfigs(of(new ConfigResource(ConfigResource.Type.BROKER, "1"))).all().get();
        configResourceConfigMap.values().stream().findFirst().get().entries().stream().filter(x -> x.name().contains("advertise")).forEach(e -> {
            log.info("Config: {} = {}", e.name(), e.value());
        });

//        killFirstConsumerConnection();

        {
            send(numberToSend);
            ConsumerRecords<String, String> poll = getPoll();
            assertThat(poll).hasSize(numberToSend);
        }

//        killFirstConsumerConnection();
        reduceConnectionToZero();

        {
            send(numberToSend);
            ConsumerRecords<String, String> poll = getPoll();
            assertThat(poll).hasSize(0);
        }

//        restoreFirstConsumerConnection();
        restoreConnectionBandwidth();

        {
            send(numberToSend);
            ConsumerRecords<String, String> poll = getPoll();
            assertThat(poll).hasSize(numberToSend * 2);
        }
    }

    private ConsumerRecords<String, String> getPoll() {
        log.debug("Polling");
        ConsumerRecords<String, String> poll = firstConsumer.poll(Duration.ofSeconds(30));
        log.debug("Polled");
        return poll;
    }

    private void send(int numberToSend) throws InterruptedException, ExecutionException {
        kcu.produceMessages(topicName, numberToSend);
    }


    /**
     * Sanity check for the type of error reported when committing offsets for partitions which aren't assigned to the
     * consumer
     *
     * @see https://github.com/confluentinc/parallel-consumer/issues/203
     */
    @SneakyThrows
    @Test
    void consumerOffsetCommitter() {
        kcu.getAdmin().createTopics(of(newTopic)).all().get();

        KafkaConsumer<String, String> c1 = kcu.createNewConsumer(topicName);
        KafkaConsumer<String, String> c2 = kcu.createNewConsumer(topicName);

        c1.subscribe(topicList);
        c2.subscribe(topicList);

        var numberToSend = 5;
        send(numberToSend);

        var all = new ArrayList<ConsumerRecord<String, String>>();
        var timeout = Duration.ofMillis(1000);
        ConsumerRecords<String, String> poll = c1.poll(timeout);
        ConsumerRecords<String, String> poll1 = c2.poll(timeout);

        all.addAll(StreamEx.of(poll.iterator()).toList());
        all.addAll(StreamEx.of(poll1.iterator()).toList());

        assertThat(all).hasSize(numberToSend);

        TopicPartition tp = new TopicPartition(topicName, 0);
        OffsetAndMetadata v1 = new OffsetAndMetadata(1);

        catchAndWrap(() -> c1.commitSync(Map.of(tp, v1)));

        try {
            catchAndWrap(() -> c2.commitSync(Map.of(tp, v1)));
        } catch (Exception e) {
            log.error("{}", ExceptionUtils.getStackTrace(e));
        }

        var poll3 = c1.poll(timeout);
        var poll4 = c2.poll(timeout);
        assertThat(poll3).isEmpty();
        assertThat(poll4).isEmpty();

        //
        catchAndWrap(() -> c1.commitSync(Map.of(tp, v1)));
        catchAndWrap(() -> c2.commitSync(Map.of(tp, v1)));
    }


    /**
     * Assert the behaiviour of the consumer when connection goes down and up
     */
    @SneakyThrows
    @Test
    void consumerOffsetCommitterTwo() {
        kcu.getAdmin().createTopics(of(newTopic)).all().get();

        var overridingOptions = new Properties();
        var brokerProxy = getBrokerProxy();
        var proxyPort = brokerProxy.getProxyPort();
        var proxiedBootstrapServers = kcu.getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        firstConsumer = kcu.createNewConsumer(topicName, overridingOptions);
        firstConsumer.subscribe(topicList);
        firstConsumer.enforceRebalance(); // todo remove

        send(numberToSend);

        var all = new ArrayList<ConsumerRecord<String, String>>();
        ConsumerRecords<String, String> poll = getPoll();
        assertThat(poll).hasSize(numberToSend);

        //
        assertThat(firstConsumer.assignment()).containsExactly(tp);

        all.addAll(StreamEx.of(poll.iterator()).toList());

        assertThat(all).hasSize(numberToSend);

        // create second consumer and being polling
        secondConsumer = kcu.createNewConsumer(topicName);
        secondConsumer.subscribe(topicList);
        ConsumerRecords<String, String> poll1 = secondConsumer.poll(timeout);

        // check assignment
        assertAssignment(FIRST);

        // poll first consumer
        ConsumerRecords<String, String> poll15 = firstConsumer.poll(timeout);

        // check assignment
        assertAssignment(FIRST);
        assertThat(firstConsumer.assignment()).containsExactly(tp);
        assertThat(secondConsumer.assignment()).isEmpty();

        //
        killFirstConsumerConnection();

        // commit first - should fail as connection closed
        assertThrows(InternalRuntimeException.class, () -> catchAndWrap(() -> firstConsumer.commitSync(offsetZeroMetaTp)));

        // check assignment
        assertThat(firstConsumer.assignment()).containsExactly(tp);
        assertThat(secondConsumer.assignment()).isEmpty();

        // commit second
        try {
            catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));
        } catch (Exception e) {
            log.error("{}", ExceptionUtils.getStackTrace(e));
        }

        // check assignment
        assertAssignment(FIRST);

        // try poll
        var poll3 = firstConsumer.poll(timeout);
        var poll4 = secondConsumer.poll(timeout);
        assertThat(poll3).isEmpty();
        assertThat(poll4).hasSize(numberToSend);

        // both consumer now think they have the partition assigned
        assertAssignment(BOTH);

        // check assignment
        assertAssignment(BOTH);

        // try to commit both
        assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> firstConsumer.commitSync(offsetZeroMetaTp));
        catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));

        // check assignment
        assertAssignment(BOTH);

        // restore broker connection for first consumer
        restoreFirstConsumerConnection();

        // try to commit both
        assertThrows(CommitFailedException.class, () -> firstConsumer.commitSync(offsetZeroMetaTp));
        catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));

        // rebalance has occurred - moves back to first consumer
        await().atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
//                    send(1);
                    firstConsumer.poll(timeout);
                    secondConsumer.poll(timeout);
                    assertAssignment(FIRST);
                });

        // commit both
        catchAndWrap(() -> firstConsumer.commitSync(offsetZeroMetaTp));
        catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));

        // check assignment
        assertAssignment(FIRST);
    }

    enum AssignmentState {
        FIRST,
        SECOND,
        BOTH,
        NONE
    }

    private void assertAssignment(AssignmentState assignment) {
        var firstAssignment = firstConsumer.assignment();
        var secondAssignment = secondConsumer.assignment();
        log.debug("firstAssignment: {}, secondAssignment: {}", firstAssignment, secondAssignment);
        switch (assignment) {
            case FIRST -> {
                assertThat(firstAssignment).containsExactly(tp);
                assertThat(secondAssignment).isEmpty();
            }
            case SECOND -> {
                assertThat(firstAssignment).isEmpty();
                assertThat(secondAssignment).containsExactly(tp);
            }
            case BOTH -> {
                assertThat(firstAssignment).containsExactly(tp);
                assertThat(secondAssignment).containsExactly(tp);
            }
            case NONE -> {
                assertThat(firstAssignment).isEmpty();
                assertThat(secondAssignment).isEmpty();
            }
        }
    }

    @SneakyThrows
    private void killFirstConsumerConnection() {
        log.warn("Killing first consumer connection via proxy disable");
        Proxy proxy = getProxy();
        proxy.disable();
        Thread.sleep(1000);
    }

    private void reduceConnectionToZero() {
        log.debug("Reducing connection to zero");
        getBrokerProxy().setConnectionCut(true);
    }

    @NonNull
    private Proxy getProxy() throws IOException {
        var port = getToxiproxy().getControlPort();
        var host = getToxiproxy().getHost();
        ToxiproxyClient client = new ToxiproxyClient(host, port);
        var proxies = client.getProxies();
        var proxy = proxies.stream().findFirst().get();
        return proxy;
    }

    @SneakyThrows
    private void restoreFirstConsumerConnection() {
        log.warn("Restore first consumer connection via proxy enable");

        Proxy proxy = getProxy();
        proxy.enable();
        Thread.sleep(1000);
    }

    private void restoreConnectionBandwidth() {
        log.warn("Restore first consumer bandwidth via proxy");
        getBrokerProxy().setConnectionCut(false);
    }

}

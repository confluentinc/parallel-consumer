package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.internal.ConsumerManager;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.JavaUtils.catchAndWrap;
import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_FOREVER;
import static io.confluent.parallelconsumer.integrationTests.OffsetCommitTest.AssignmentState.BOTH;
import static io.confluent.parallelconsumer.integrationTests.OffsetCommitTest.AssignmentState.FIRST;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GROUP_SESSION_TIMEOUT_MS;
import static io.confluent.parallelconsumer.internal.ConsumerManager.DEFAULT_API_TIMEOUT;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Various tests around disconnects and reconnects
 *
 * @author Antony Stubbs
 */
@Tag("disconnect")
@Tag("toxiproxy")
@Slf4j
class OffsetCommitTest extends BrokerIntegrationTest {

    public static final int TIMEOUT = 30;
    public static final Duration TIMEOUT_DURATION = ofSeconds(TIMEOUT);
    KafkaClientUtils kcu = getKcu();
    String topicName = getClass().getName() + "-" + System.currentTimeMillis();
    List<String> topicList = of(topicName);
    TopicPartition tp = new TopicPartition(topicName, 0);
    OffsetAndMetadata offsetZeroMeta = new OffsetAndMetadata(0);
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
    Map<TopicPartition, OffsetAndMetadata> offsetZeroMetaTp = Map.of(tp, offsetZeroMeta);
    Duration timeout = ofSeconds(1);
    int numberToSend = 5;
    KafkaConsumer<String, String> proxiedConsumer;
    KafkaConsumer<String, String> secondConsumer;


    /**
     * Tests directly how the {@link KafkaConsumer} handles disconnects and reconnects
     * <p>
     * Most basic version
     */
    @SneakyThrows
    @Test
    void canTerminateConsumerConnection() {
        proxiedConsumer = createProxiedConsumer(topicName);
        proxiedConsumer.subscribe(topicList);
        proxiedConsumer.enforceRebalance(); // todo remove?

        logAdvertisedListeners();

        {
            send(numberToSend);
            ConsumerRecords<String, String> poll = getPoll();
            assertThat(poll).hasSize(numberToSend);
        }

        killProxyConnection();

        {
            send(numberToSend);
            var groupSessionTimeout = ofMillis(GROUP_SESSION_TIMEOUT_MS);
            await()
                    .pollDelay(groupSessionTimeout)
                    .atMost(ofMillis(GROUP_SESSION_TIMEOUT_MS * 2))
                    .untilAsserted(() -> {
                        var poll = getPoll(groupSessionTimeout); // moderate poll timeout
                        assertThat(poll).hasSize(0);
                    });
        }

        restoreProxyConnection();

        {
            send(numberToSend);
            await().untilAsserted(() -> {
                ConsumerRecords<String, String> poll = getPoll(TIMEOUT_DURATION); // should return very quickly once connection reestablished
                assertThat(poll).hasSize(numberToSend * 2);
            });
        }
    }

    /**
     * debug info - describe the clusters advertised listeners
     */
    @SneakyThrows
    private void logAdvertisedListeners() {
        var configResourceConfigMap = kcu.getAdmin().describeConfigs(of(new ConfigResource(ConfigResource.Type.BROKER, "1"))).all().get();
        configResourceConfigMap.values().stream().findFirst().get().entries().stream().filter(x -> x.name().contains("advertise")).forEach(e -> {
            log.info("Config: {} = {}", e.name(), e.value());
        });
    }

    private ConsumerRecords<String, String> getPoll() {
        return getPoll(TIMEOUT_DURATION);
    }

    private ConsumerRecords<String, String> getPoll(Duration timeout) {
        log.debug("Polling");
        ConsumerRecords<String, String> poll = proxiedConsumer.poll(timeout);
        log.debug("Polled, returned {} records", poll.count());
        return poll;
    }

    private void send(int numberToSend) throws InterruptedException, ExecutionException {
        kcu.produceMessages(topicName, numberToSend);
    }

    /**
     * Assert the behaviour of the KafkaConsumer when connection goes down and up, when there are two consumers in a
     * group
     */
    @SneakyThrows
    @Test
    void consumerOffsetCommitterConnectionLoss() {
        kcu.getAdmin().createTopics(of(newTopic)).all().get();

        proxiedConsumer = createProxiedConsumer(topicName);
        proxiedConsumer.subscribe(topicList);
        proxiedConsumer.enforceRebalance(); // todo remove

        send(numberToSend);

        var all = new ArrayList<ConsumerRecord<String, String>>();
        ConsumerRecords<String, String> poll = getPoll();
        assertThat(poll).hasSize(numberToSend);

        //
        assertThat(proxiedConsumer.assignment()).containsExactly(tp);

        all.addAll(StreamEx.of(poll.iterator()).toList());

        assertThat(all).hasSize(numberToSend); // todo remove?

        // create second consumer and being polling
        secondConsumer = kcu.createNewConsumer(topicName);
        secondConsumer.subscribe(topicList);
        ConsumerRecords<String, String> poll1 = secondConsumer.poll(timeout);

        // check assignment
        assertAssignment(FIRST);

        // poll first consumer
        ConsumerRecords<String, String> poll15 = proxiedConsumer.poll(timeout);

        // check assignment
        assertAssignment(FIRST);
        assertThat(proxiedConsumer.assignment()).containsExactly(tp);
        assertThat(secondConsumer.assignment()).isEmpty();

        //
        killProxyConnection();

        // commit first - should fail as connection closed
        assertThrows(InternalRuntimeException.class, () -> catchAndWrap(() -> proxiedConsumer.commitSync(offsetZeroMetaTp)));

        // check assignment
        assertThat(proxiedConsumer.assignment()).containsExactly(tp);
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
        var poll3 = proxiedConsumer.poll(timeout);
        var poll4 = secondConsumer.poll(timeout);
        assertThat(poll3).isEmpty();
        assertThat(poll4).hasSize(numberToSend);

        // both consumer now think they have the partition assigned
        assertAssignment(BOTH);

        // check assignment
        assertAssignment(BOTH);

        // try to commit both
        assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> proxiedConsumer.commitSync(offsetZeroMetaTp));
        catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));

        // check assignment
        assertAssignment(BOTH);

        // restore broker connection for first consumer
        restoreProxyConnection();

        // try to commit both
        assertThrows(CommitFailedException.class, () -> proxiedConsumer.commitSync(offsetZeroMetaTp));
        catchAndWrap(() -> secondConsumer.commitSync(offsetZeroMetaTp));

        // rebalance has occurred - moves back to first consumer
        await().atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    proxiedConsumer.poll(timeout);
                    secondConsumer.poll(timeout);
                    assertAssignment(FIRST);
                });

        // commit both
        catchAndWrap(() -> proxiedConsumer.commitSync(offsetZeroMetaTp));
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
        var firstAssignment = proxiedConsumer.assignment();
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

    /**
     * Test Parallel Consumers behaviour in the same way - what happens when the broker connection goes down and up
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void parallelConsumerBrokerReconnectionTest(CommitMode commitMode) {
        proxiedConsumer = createProxiedConsumer(topicName);
        var processedCount = new AtomicInteger();

        final ParallelConsumerOptions.RetrySettings retrySettings = ParallelConsumerOptions.RetrySettings.builder()
                .failureReaction(RETRY_FOREVER)
                .build();

        final ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> preSettings = ParallelConsumerOptions.<String, String>builder()
                .consumer(proxiedConsumer)
                .commitMode(commitMode)
                .retrySettings(retrySettings);

        if (commitMode == PERIODIC_TRANSACTIONAL_PRODUCER) {
            preSettings.producer(createProxiedTransactionalProducer());
        }

        var options = preSettings
                .build();

        var pc = kcu.buildPc(options, null, 1);
        pc.subscribe(topicList);
        pc.poll(recordContexts -> {
            log.debug("{}", recordContexts);
            processedCount.incrementAndGet();

            if (processedCount.get() == 1) {
                log.debug("kill connection after first poll result");
                killProxyConnection();

                log.debug("Making PC try to commit the dirty state while the connection is closed...");
                pc.requestCommitAsap();
            }
        });

        //
        log.debug("allow time for pc to start polling before killing connection");
        ThreadUtils.sleepSecondsLog(1);

        //
        log.debug("to make the state dirty, so we can try committing");
        send(1);

        //
        log.debug("wait until the consumer has processed the first message");
        await().untilAsserted(() -> Truth.assertThat(processedCount.get()).isEqualTo(1));

        //
        log.debug("send some more messages which won't be able to be consumed due to being disconnected");
        send(numberToSend);

        //
        log.debug("wait for a while, making sure none of the extra 5 records make it through");
        var delay = TIMEOUT;
        await().failFast(() -> pc.isClosedOrFailed() || processedCount.get() > 1)
                .pollDelay(ofSeconds(delay))
                .timeout(ofSeconds(delay + DEFAULT_API_TIMEOUT.toSeconds() * 2)) // longer than the commit timeout
                .untilAsserted(() -> {
                    Truth.assertWithMessage("should still only be one record processed")
                            .that(processedCount.get()).isEqualTo(1);
                });

        //
        restoreProxyConnection();

        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            // system does not recover from this state
            await().untilAsserted(() -> {
                Truth.assertWithMessage("should have crashed")
                        .that(pc.isClosedOrFailed()).isTrue();
            });
            var throwableThat = assertThat(pc).getFailureCause().hasCauseThat();
            throwableThat.isInstanceOf(InternalRuntimeException.class);
            var messageThat = throwableThat.hasMessageThat();
            messageThat.contains("offsets to transaction");
            messageThat.contains("Producer");
            messageThat.contains("reinitialised");
        } else {
            //
            log.debug("Now connection is open again, wait for all records to get processed");
            await().failFast(pc::isClosedOrFailed)
                    .atMost(ConsumerManager.DEFAULT_API_TIMEOUT.multipliedBy(2))
                    .untilAsserted(() -> {
                        Truth.assertThat(processedCount.get()).isAtLeast(numberToSend);
                    });

            //
            pc.close();
            assertThat(pc).getFailureCause().isNull();
        }
        //
        log.debug("Processed: {}", processedCount.get());
    }

}
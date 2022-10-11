package io.confluent.parallelconsumer.integrationTests.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.JavaUtils;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.csid.utils.JavaUtils.getLast;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Integration test versions of {@link io.confluent.parallelconsumer.state.PartitionStateCommittedOffsetTest}, where
 * committed offset gets moved around or deleted, or random offsets are removed.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.state.PartitionStateCommittedOffsetTest
 */
@Slf4j
class PartitionStateCommittedOffsetIT extends BrokerIntegrationTest<String, String> {

    AdminClient ac;

    String groupId;

    ParallelEoSStreamProcessor<String, String> pc;

    TopicPartition tp;

    int TO_PRODUCE = 200;

    @BeforeEach
    void setup() {
        setupTopic();
        tp = new TopicPartition(getTopic(), 0);
        groupId = getKcu().getConsumer().groupMetadata().groupId();
        this.ac = getKcu().getAdmin();
    }

    /**
     * Test for offset gaps in partition data (i.e. compacted topics)
     */
    @Test
    void compactedTopic() {
        // setup our extra special compacting broker
        KafkaContainer compactingBroker = null;
        {
            KafkaContainer compactingBroker = BrokerIntegrationTest.createKafkaContainer("40000");
            compactingBroker.start();
            kcu = new KafkaClientUtils(compactingBroker);
            kcu.open();

            setup();
        }

        setupCompacted();

        var TO_PRODUCE = this.TO_PRODUCE / 10;

        List<String> keys = produceMessages(TO_PRODUCE);

        final int UNTIL_OFFSET = TO_PRODUCE / 2;
        var processedOnFirstRun = runPcUntilOffset(UNTIL_OFFSET, TO_PRODUCE, UniSets.of(TO_PRODUCE - 3L));
        assertWithMessage("Last processed should be at least half of the total sent, so that there is incomplete data to track")
                .that(getLast(processedOnFirstRun).get().offset())
                .isGreaterThan(TO_PRODUCE / 2);

        // commit offset
        closePC();

        //
        ArrayList<String> tombStonedKeysRaw = sendRandomTombstones(keys, TO_PRODUCE);
        Set<String> tombStonedKeys = new HashSet<>(tombStonedKeysRaw);

        var processedOnFirstRunWithTombstoneTargetsRemoved = processedOnFirstRun.stream()
                .filter(context -> !tombStonedKeys.contains(context.key()))
                .map(PollContext::key)
                .collect(Collectors.toList());

        var firstRunPartitioned = processedOnFirstRun.stream().collect(Collectors.partitioningBy(context -> tombStonedKeys.contains(context.key())));
        var saved = firstRunPartitioned.get(Boolean.FALSE);
        var tombstoned = firstRunPartitioned.get(Boolean.TRUE);
        log.debug("kept offsets: {}", saved.stream().mapToLong(PollContext::offset).boxed().collect(Collectors.toList()));
        log.debug("kept keys: {}", saved.stream().map(PollContext::key).collect(Collectors.toList()));
        log.debug("tombstoned offsets: {}", tombstoned.stream().map(PollContext::key).collect(Collectors.toList()));
        log.debug("tombstoned keys: {}", tombstoned.stream().mapToLong(PollContext::offset).boxed().collect(Collectors.toList()));


        var tombstoneTargetOffsetsFromFirstRun = tombstoned.stream()
                .filter(context -> tombStonedKeys.contains(context.key()))
                .map(PollContext::offset)
                .collect(Collectors.toList());

        var tombStonedOffsetsFromKey = tombStonedKeys.stream()
                .map(PartitionStateCommittedOffsetIT::getOffsetFromKey).collect(Collectors.toList());
        log.debug("First run produced, with tombstone targets removed: {}", processedOnFirstRunWithTombstoneTargetsRemoved);

        //
        triggerTombStoneProcessing();

        // The offsets of the tombstone targets should not be read in second run
        final int expectedTotalNumberRecordsProduced = TO_PRODUCE + tombStonedOffsetsFromKey.size();
        final int expectedOffsetProcessedToSecondRun = TO_PRODUCE + tombStonedKeys.size();
        var processedOnSecondRun = runPcUntilOffset(expectedOffsetProcessedToSecondRun).stream()
                .filter(recordContexts -> !recordContexts.key().contains("compaction-trigger"))
                .collect(Collectors.toList());

        //
        List<String> offsetsFromSecondRunFromKey = processedOnSecondRun.stream()
                .map(PollContext::key)
                .collect(Collectors.toList());

        assertWithMessage("All keys should still exist")
                .that(offsetsFromSecondRunFromKey)
                .containsAtLeastElementsIn(processedOnFirstRun.stream().map(PollContext::key).collect(Collectors.toList()));

        //
        List<Long> offsetsFromSecond = processedOnSecondRun.stream()
                .map(PollContext::offset)
                .collect(Collectors.toList());

        assertWithMessage("The offsets of the tombstone targets should not be read in second run")
                .that(offsetsFromSecond)
                .containsNoneIn(tombstoneTargetOffsetsFromFirstRun);

        compactingBroker.close();
    }

    private ArrayList<PollContext<String, String>> runPcUntilOffset(int offset) {
        return runPcUntilOffset(offset, offset, UniSets.of());
    }

    private static long getOffsetFromKey(String key) {
        return Long.parseLong(key.substring(key.indexOf("-") + 1));
    }

    @SneakyThrows
    private void setupCompacted() {
        log.debug("Setting up aggressive compaction...");
        ConfigResource topicConfig = new ConfigResource(ConfigResource.Type.TOPIC, getTopic());

        Collection<AlterConfigOp> alterConfigOps = new ArrayList<>();

        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "1"), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0"), AlterConfigOp.OpType.SET));

        var configs = UniMaps.of(topicConfig, alterConfigOps);
        KafkaFuture<Void> all = ac.incrementalAlterConfigs(configs).all();
        all.get(5, SECONDS);

        log.debug("Compaction setup complete");
    }

    @SneakyThrows
    private void triggerTombStoneProcessing() {
        // send a lot of messages to fill up segments
        List<String> keys = produceMessages(TO_PRODUCE * 2, "log-compaction-trigger-");
        // or wait?
        ThreadUtils.sleepSecondsLog(10);
    }

    @SneakyThrows
    private ArrayList<String> sendRandomTombstones(List<String> keys, int howMany) {
        var tombstoneKeys = new ArrayList<String>();
        // fix randomness
        List<Future<RecordMetadata>> futures = JavaUtils.getRandom(keys, howMany).stream()
                .map((String key) -> {
                    tombstoneKeys.add(key);
                    var tombstone = new ProducerRecord<>(getTopic(), key, "tombstone");
                    return getKcu().getProducer()
                            .send(tombstone);
                })
                .collect(Collectors.toList());
        List<Long> tombstoneOffsets = new ArrayList<>();
        for (Future<RecordMetadata> future : futures) {
            RecordMetadata recordMetadata = future.get(5, SECONDS);
            tombstoneOffsets.add(recordMetadata.offset());
        }

        tombstoneKeys.sort(Comparator.comparingLong(PartitionStateCommittedOffsetIT::getOffsetFromKey));

        log.debug("Keys to tombstone: {}\n" +
                        "Offsets of the generated tombstone: {}",
                tombstoneKeys,
                tombstoneOffsets);
        return tombstoneKeys;
    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay) (metdata lost?)
     */
    @Test
    void committedOffsetLower() {
        produceMessages(TO_PRODUCE);

        runPcUntilOffset(50);

        closePC();

        final int moveToOffset = 25;

        moveCommittedOffset(moveToOffset);

        runPcCheckStartIs(moveToOffset, TO_PRODUCE);
    }

    /**
     * Ensure that the PC starts at the correct offset
     *
     * @param targetStartOffset the offset to check that PC starts at
     * @param checkUpTo         the offset to run the PC until, while checking for the start offset
     */
    private void runPcCheckStartIs(long targetStartOffset, long checkUpTo) {
        this.pc = super.getKcu().buildPc(PARTITION);
        pc.subscribe(of(getTopic()));

        AtomicLong lowest = new AtomicLong(Long.MAX_VALUE);
        AtomicLong highest = new AtomicLong();

        pc.poll(recordContexts -> {
            long thisOffset = recordContexts.offset();
            if (thisOffset < lowest.get()) {
                log.debug("Found lowest offset {}", thisOffset);
                lowest.set(thisOffset);
            } else {
                highest.set(thisOffset);
            }
        });

        Awaitility.await().untilAtomic(highest, equalTo(checkUpTo - 1));

        pc.close();

        assertWithMessage("Offset started as").that(lowest.get()).isEqualTo(targetStartOffset);
    }

    @SneakyThrows
    private void moveCommittedOffset(long offset) {
        log.debug("Moving offset to {}", offset);
        var data = UniMaps.of(tp, new OffsetAndMetadata(offset));
        var result = ac.alterConsumerGroupOffsets(groupId, data);
        result.all().get(5, SECONDS);
        log.debug("Moved offset to {}", offset);
    }

    private void closePC() {
        pc.close();
    }

    private ArrayList<PollContext<String, String>> runPcUntilOffset(long succeedUpToOffset, long expectedProcessToOffset, Set<Long> exceptionsToSucceed) {
        log.debug("Running PC until offset {}", succeedUpToOffset);
        this.pc = super.getKcu().buildPc(UNORDERED, GroupOption.NEW_GROUP);

        SortedSet<PollContext<String, String>> seenOffsets = Collections.synchronizedSortedSet(new TreeSet<>(Comparator.comparingLong(PollContext::offset)));
        AtomicLong succeededUpTo = new AtomicLong();
        pc.subscribe(of(getTopic()));
        pc.poll(pollContext -> {
            seenOffsets.add(pollContext);
            long thisOffset = pollContext.offset();
            if (exceptionsToSucceed.contains(thisOffset)) {
                log.debug("Exceptional offset {} succeeded", thisOffset);
            } else if (thisOffset >= succeedUpToOffset) {
                log.debug("Failing on {}", thisOffset);
                throw new RuntimeException("Failing on " + thisOffset);
            } else {
                succeededUpTo.set(thisOffset);
                log.debug("Succeeded {}", thisOffset);
            }
        });

        Awaitility.await().untilAsserted(() -> {
            assertThat(seenOffsets).isNotEmpty();
            assertThat(seenOffsets.last().offset()).isGreaterThan(expectedProcessToOffset - 2);
        });
        log.debug("Consumed up to {}", succeedUpToOffset);

        pc.close();

        var sorted = new ArrayList<>(seenOffsets);
        Collections.sort(sorted, Comparator.comparingLong(PollContext::offset));
        return sorted;
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metdata lost?)
     */
    @Test
    void committedOffsetHigher() {
        final int quantity = 100;
        produceMessages(quantity);

        runPcUntilOffset(50);

        closePC();

        final int moveToOffset = 75;

        moveCommittedOffset(moveToOffset);

        runPcCheckStartIs(moveToOffset, quantity);
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     */
    @Test
    void committedOffsetRemoved() {
        produceMessages(TO_PRODUCE);

        final int END_OFFSET = 50;
        runPcUntilOffset(END_OFFSET);

        closePC();

        causeCommittedOffsetToBeRemoved(END_OFFSET);

        produceMessages(TO_PRODUCE);

        final int TOTAL = TO_PRODUCE * 2;
        runPcCheckStartIs(END_OFFSET + 1, TOTAL);
    }

    private void causeCommittedOffsetToBeRemoved(long offset) {
        throw new RuntimeException();
    }

    @Test
    void cgOffsetsDeletedResetLatest() {
        produceMessages(TO_PRODUCE);

        final int END_OFFSET = 50;
        runPcUntilOffset(END_OFFSET);

        closePC();

        causeCommittedConsumerGroupOffsetToBeDeleted();

        produceMessages(TO_PRODUCE);

        final int TOTAL_PRODUCED = TO_PRODUCE * 2;
        runPcCheckStartIs(TOTAL_PRODUCED, TOTAL_PRODUCED);
    }

    @Test
    void cgOffsetsDeletedResetEarliest() {
        produceMessages(TO_PRODUCE);

        final int END_OFFSET = 50;
        runPcUntilOffset(END_OFFSET);

        closePC();

        causeCommittedConsumerGroupOffsetToBeDeleted();

        produceMessages(100);

        runPcCheckStartIs(0, TO_PRODUCE);
    }

    private void causeCommittedConsumerGroupOffsetToBeDeleted() {
        throw new RuntimeException();
    }


}
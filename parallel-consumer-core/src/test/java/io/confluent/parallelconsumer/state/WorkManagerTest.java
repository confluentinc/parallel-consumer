package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.AdvancingWallClockProvider;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.time.Duration.ofSeconds;
import static java.util.Comparator.comparingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see WorkManager
 */
@Slf4j
class WorkManagerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    WorkManager<String, String> wm;

    int offset;

    Instant time = Instant.now();

    AdvancingWallClockProvider testClock = new AdvancingWallClockProvider() {
        @Override
        public Instant getNow() {
            return time;
        }
    };

    @BeforeEach
    public void setup() {
        setupWorkManager(ParallelConsumerOptions.builder().build());
    }

    protected List<WorkContainer<String, String>> successfulWork = new ArrayList<>();

    private void setupWorkManager(ParallelConsumerOptions build) {
        offset = 0;

        wm = new WorkManager<>(build, new MockConsumer<>(OffsetResetStrategy.EARLIEST), testClock);
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Heard some successful work: {}", work);
            successfulWork.add(work);
        });
        int partition = 0;
        assignPartition(partition);
    }

    private void assignPartition(final int partition) {
        wm.onPartitionsAssigned(UniLists.of(new TopicPartition(INPUT_TOPIC, partition)));
    }

    /**
     * Adds 3 units of work
     */
    private void registerSomeWork() {
        String key = "key-0";
        int partition = 0;
        var rec0 = makeRec("0", key, partition);
        var rec1 = makeRec("1", key, partition);
        var rec2 = makeRec("2", key, partition);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec0, rec1, rec2));
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(recs);
    }

    private ConsumerRecord<String, String> makeRec(String value, String key, int partition) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, partition, offset, key, value);
        offset++;
        return stringStringConsumerRecord;
    }

    @Test
    void testRemovedUnordered() {
        setupWorkManager(ParallelConsumerOptions.builder().ordering(UNORDERED).build());
        registerSomeWork();

        int max = 1;
        var gottenWork = wm.getWorkIfAvailable(max);
        assertThat(gottenWork).hasSize(1);
        assertOffsets(gottenWork, of(0));

        wm.onSuccess(gottenWork.get(0));

        gottenWork = wm.getWorkIfAvailable(max);
        assertThat(gottenWork).hasSize(1);
        assertOffsets(gottenWork, of(1));
    }

    @Test
    void testUnorderedAndDelayed() {
        setupWorkManager(ParallelConsumerOptions.builder().ordering(UNORDERED).build());
        registerSomeWork();

        int max = 2;

        var works = wm.getWorkIfAvailable(max);
        assertThat(works).hasSize(2);
        assertOffsets(works, of(0, 1));

        wm.onSuccess(works.get(0));
        wm.onFailure(works.get(1));

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(2));

        wm.onSuccess(works.get(0));

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of());

        advanceClockBySlightlyLessThanDelay();

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of());

        advanceClockByDelay();

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1));
        wm.onSuccess(works.get(0));

        assertThat(successfulWork)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0, 2, 1));
    }

    /**
     * Checks the offsets of the work, matches the offsets in the provided list
     */
    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>>
    assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected) {
        return assertThat(works)
                .as("offsets of work given")
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(expected);
    }

    @Test
    public void testOrderedInFlightShouldBlockQueue() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int max = 2;

        var works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(0));
        var w = works.get(0);

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of()); // should be blocked by in flight

        wm.onSuccess(w);

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1));
    }

    /**
     * Tests failed work delay
     */
    @Test
    void testOrderedAndDelayed() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        // sanity
        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int maxWorkToGet = 2;

        var works = wm.getWorkIfAvailable(maxWorkToGet);

        assertOffsets(works, of(0));

        // fail the work
        var wc = works.get(0);
        wm.onFailure(wc);

        // nothing available to get
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // advance clock to make delay pass
        advanceClockByDelay();

        // work should now be ready to take
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));

        wc = works.get(0);
        wm.onFailure(wc);

        advanceClock(wc.getRetryDelayConfig().minus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // increased advance to allow for bigger delay under high load during parallel test execution.
        advanceClock(wc.getRetryDelayConfig().plus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));
        wm.onSuccess(works.get(0));

        assertOffsets(successfulWork, of(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(1));
        wm.onSuccess(works.get(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(2));
        wm.onSuccess(works.get(0));

        // check all published in the end
        assertOffsets(successfulWork, of(0, 1, 2));
    }

    @Test
    void containerDelay() {
        var wc = new WorkContainer<String, String>(0, null);
        assertThat(wc.hasDelayPassed(testClock)).isTrue(); // when new, there's no delay
        wc.fail(testClock);
        assertThat(wc.hasDelayPassed(testClock)).isFalse();
        advanceClockBySlightlyLessThanDelay();
        assertThat(wc.hasDelayPassed(testClock)).isFalse();
        advanceClockByDelay();
        boolean actual = wc.hasDelayPassed(testClock);
        assertThat(actual).isTrue();
    }

    private void advanceClockBySlightlyLessThanDelay() {
        Duration retryDelay = WorkContainer.defaultRetryDelay;
        Duration duration = retryDelay.dividedBy(2);
        time = time.plus(duration);
    }

    private void advanceClockByDelay() {
        time = time.plus(WorkContainer.defaultRetryDelay);
    }

    private void advanceClock(Duration by) {
        time = time.plus(by);
    }

    @Test
    void insertWrongOrderPreservesOffsetOrdering() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder().ordering(UNORDERED).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(UNORDERED);

        registerSomeWork();

        String key = "key";
        int partition = 0;

        // mess with offset order for insertion
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, key, "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, key, "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, key, "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        int max = 10;

        var works = wm.getWorkIfAvailable(4);
        assertOffsets(works, of(0, 1, 2, 6));

        // fail some
        wm.onFailure(works.get(1));
        wm.onFailure(works.get(3));

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(8, 10));

        //
        advanceClockByDelay();

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1, 6));
    }

    @Test
    @Disabled
    public void maxPerPartition() {
    }

    @Test
    @Disabled
    public void maxPerTopic() {
    }

    @Test
    public void maxInFlight() {
        //
        var opts = ParallelConsumerOptions.builder();
        setupWorkManager(opts.build());

        //
        registerSomeWork();

        //
        assertThat(wm.getWorkIfAvailable()).hasSize(1);
        assertThat(wm.getWorkIfAvailable()).isEmpty();
    }

    static class FluentQueue<T> implements Iterable<T> {
        ArrayDeque<T> work = new ArrayDeque<>();

        Collection<T> add(Collection<T> c) {
            work.addAll(c);
            return c;
        }

        public T poll() {
            return work.poll();
        }

        @Override
        public Iterator<T> iterator() {
            return work.iterator();
        }

        public int size() {
            return work.size();
        }
    }

    @Test
    @Disabled
    public void multipleFailures() {
    }


    @Test
    @Disabled
    public void delayedOrdered() {
    }

    @Test
    @Disabled
    public void delayedUnordered() {
    }

    @Test
    void orderedByPartitionsParallel() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder()
                .ordering(PARTITION)
                .build();
        setupWorkManager(build);

        registerSomeWork();

        var partition = 2;
        assignPartition(2);
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "66", "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "66", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "66", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        var works = wm.getWorkIfAvailable();
        assertOffsets(works, of(0, 6));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        assertOffsets(works, of(1, 8));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        assertOffsets(works, of(2, 10));
        successAll(works);
    }

    private void successAll(List<WorkContainer<String, String>> works) {
        for (WorkContainer<String, String> work : works) {
            wm.onSuccess(work);
        }
    }

    @Test
    void orderedByKeyParallel() {
        var build = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(KEY);

        registerSomeWork();

        var partition = 2;
        assignPartition(2);
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "key-a", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "key-b", "value");
        var rec0 = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "key-a", "value");
        var rec4 = new ConsumerRecord<>(INPUT_TOPIC, partition, 12, "key-c", "value");
        var rec5 = new ConsumerRecord<>(INPUT_TOPIC, partition, 15, "key-a", "value");
        var rec6 = new ConsumerRecord<>(INPUT_TOPIC, partition, 20, "key-c", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec2, rec3, rec0, rec4, rec5, rec6));
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        var works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder()); // we actually don't care about the order
        // one record per key
        assertOffsets(works, of(0, 6, 8, 12));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(1, 10, 20));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(2, 15));
        successAll(works);

        works = wm.getWorkIfAvailable();
        assertOffsets(works, of());
    }

    @Test
    @Disabled
    public void unorderedPartitionsGreedy() {
    }

    //        @Test
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 10, 20, 30, 50, 1000})
    void highVolumeKeyOrder(int quantity) {
        int uniqueKeys = 100;

        var build = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupWorkManager(build);

        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        List<Integer> keys = range(uniqueKeys).list();

        var records = ktu.generateRecords(keys, quantity);
        var flattened = ktu.flatten(records.values());
        flattened.sort(comparingLong(ConsumerRecord::offset));

        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, 0), flattened);
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        List<WorkContainer<String, String>> work = wm.getWorkIfAvailable();

        //
        assertThat(work).hasSameSizeAs(records.keySet());
    }

    @Test
    void treeMapOrderingCorrect() {
        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        int i = 10;
        var records = ktu.generateRecords(i);

        var treeMap = new TreeMap<Long, WorkContainer<String, String>>();
        for (ConsumerRecord<String, String> record : records) {
            treeMap.put(record.offset(), new WorkContainer<>(0, record));
        }

        // read back, assert correct order
        NavigableSet<Long> ascendingOrder = treeMap.navigableKeySet();
        Object[] objects = ascendingOrder.toArray();

        assertThat(objects).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    /**
     * Checks work management is correct in this respect.
     */
    @Test
    public void workQueuesEmptyWhenAllWorkComplete() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .build();
        setupWorkManager(build);
        registerSomeWork();

        //
        var work = wm.getWorkIfAvailable();
        assertThat(work).hasSize(3);

        //
        for (var w : work) {
            w.onUserFunctionSuccess();
            wm.onSuccess(w);
        }

        //
        assertThat(wm.getSm().getNumberOfWorkQueuedInShardsAwaitingSelection()).isZero();
        assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(3);

        // drain commit queue
        var completedFutureOffsets = wm.findCompletedEligibleOffsetsAndRemove();
        assertThat(completedFutureOffsets).hasSize(1); // coalesces (see log)
        assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(0);
    }

    /**
     * Tests that the resuming iterator is used correctly
     */
    @ParameterizedTest
    @EnumSource
    void resumesFromNextShard(ParallelConsumerOptions.ProcessingOrder order) {
        Assumptions.assumeFalse(order == KEY); // just want to test ordered vs unordered

        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder()
                .ordering(order)
                .build();
        setupWorkManager(build);

        registerSomeWork();

        assignPartition(1);
        assignPartition(2);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        var rec = new ConsumerRecord<>(INPUT_TOPIC, 1, 11, "11", "value");
        m.put(new TopicPartition(INPUT_TOPIC, 1), of(rec));
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, 2, 21, "21", "value");
        m.put(new TopicPartition(INPUT_TOPIC, 2), of(rec2));
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(recs);

        // force ingestion of records - see refactor: Queue unification #219
        wm.tryToEnsureQuantityOfWorkQueuedAvailable(100);

        var workContainersOne = wm.getWorkIfAvailable(1);
        var workContainersTwo = wm.getWorkIfAvailable(1);
        var workContainersThree = wm.getWorkIfAvailable(1);
        var workContainersFour = wm.getWorkIfAvailable(1);

        Truth.assertThat(workContainersOne).hasSize(1);
        Truth.assertThat(workContainersOne.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(0);
        Truth.assertThat(workContainersTwo).hasSize(1);
        Truth.assertThat(workContainersTwo.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(1);
        Truth.assertThat(workContainersThree).hasSize(1);
        Truth.assertThat(workContainersThree.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(2);

        if (order == PARTITION) {
            Truth.assertThat(workContainersFour).isEmpty();
        } else {
            Truth.assertThat(workContainersFour).hasSize(1);
            Optional<WorkContainer<String, String>> work = workContainersFour.stream().findFirst();
            Truth.assertThat(work.get().getTopicPartition().partition()).isEqualTo(0);
            Truth.assertThat(work.get().offset()).isEqualTo(1);
            Truth.assertThat(work.get().getCr().value()).isEqualTo("1");
        }
    }

}

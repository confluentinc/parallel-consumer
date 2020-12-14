package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.AdvancingWallClockProvider;
import io.confluent.csid.utils.KafkaTestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static io.confluent.parallelconsumer.WorkContainer.getRetryDelay;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see WorkManager
 */
@Slf4j
public class WorkManagerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    WorkManager<String, String> wm;

    int offset;

    Instant time = Instant.now();

    AdvancingWallClockProvider clock = new AdvancingWallClockProvider() {
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

        wm = new WorkManager<>(build, new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        wm.setClock(clock);
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Heard some successful work: {}", work);
            successfulWork.add(work);
        });
    }

    /**
     * Adds 3 units of work
     */
    private void registerSomeWork() {
        String key = "key-0";
        int partition = 0;
        var rec = makeRec("0", key, partition);
        var rec2 = makeRec("1", key, partition);
        var rec3 = makeRec("2", key, partition);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec, rec2, rec3));
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(recs);
    }

    @NotNull
    private ConsumerRecord<String, String> makeRec(String value, String key, int partition) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, partition, offset, key, value);
        offset++;
        return stringStringConsumerRecord;
    }

    @Test
    public void testRemovedUnordered() {
        setupWorkManager(ParallelConsumerOptions.builder().ordering(UNORDERED).build());
        registerSomeWork();

        int max = 1;
        var works = wm.maybeGetWork(max);
        assertThat(works).hasSize(1);
        assertOffsets(works, of(0));

        wm.success(works.get(0));

        works = wm.maybeGetWork(max);
        assertThat(works).hasSize(1);
        assertOffsets(works, of(1));
    }

    @Test
    public void testUnorderedAndDelayed() {
        setupWorkManager(ParallelConsumerOptions.builder().ordering(UNORDERED).build());
        registerSomeWork();


        int max = 2;

        var works = wm.maybeGetWork(max);
        assertThat(works).hasSize(2);
        assertOffsets(works, of(0, 1));

        wm.success(works.get(0));
        wm.failed(works.get(1));

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(2));

        wm.success(works.get(0));

        works = wm.maybeGetWork(max);
        assertOffsets(works, of());

        advanceClockBySlightlyLessThanDelay();

        works = wm.maybeGetWork(max);
        assertOffsets(works, of());

        advanceClockByDelay();

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(1));
        wm.success(works.get(0));

        assertThat(successfulWork)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0, 2, 1));
    }

    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>> assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected) {
        return assertThat(works)
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

        var works = wm.maybeGetWork(max);
        assertOffsets(works, of(0));
        var w = works.get(0);

        works = wm.maybeGetWork(max);
        assertOffsets(works, of()); // should be blocked by in flight

        wm.success(w);

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(1));
    }

    @Test
    public void testOrderedAndDelayed() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int max = 2;

        var works = wm.maybeGetWork(max);
        assertOffsets(works, of(0));
        var wc = works.get(0);
        wm.failed(wc);

        works = wm.maybeGetWork(max);
        assertOffsets(works, of());

        advanceClockByDelay();

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(0));

        wc = works.get(0);
        wm.failed(wc);

        advanceClock(getRetryDelay().minus(ofSeconds(1)));

        works = wm.maybeGetWork(max);
        assertOffsets(works, of());

        advanceClock(getRetryDelay());

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(0));
        wm.success(works.get(0));

        assertOffsets(successfulWork, of(0));

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(1));
        wm.success(works.get(0));

        works = wm.maybeGetWork(max);
        assertOffsets(works, of(2));
        wm.success(works.get(0));

        // check all published in the end
        assertOffsets(successfulWork, of(0, 1, 2));
    }

    @Test
    public void containerDelay() {
        var wc = new WorkContainer<String, String>(null);
        assertThat(wc.hasDelayPassed(clock)).isTrue(); // when new no delay
        wc.fail(clock);
        assertThat(wc.hasDelayPassed(clock)).isFalse();
        advanceClockBySlightlyLessThanDelay();
        assertThat(wc.hasDelayPassed(clock)).isFalse();
        advanceClockByDelay();
        assertThat(wc.hasDelayPassed(clock)).isTrue();
    }

    private void advanceClockBySlightlyLessThanDelay() {
        Duration retryDelay = getRetryDelay();
        Duration duration = retryDelay.dividedBy(2);
        time = time.plus(duration);
    }

    private void advanceClockByDelay() {
        time = time.plus(getRetryDelay());
    }

    private void advanceClock(Duration by) {
        time = time.plus(by);
    }

    @Test
    public void insertWrongOrderPreservesOffsetOrdering() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(UNORDERED).build();
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

        var works = wm.maybeGetWork(4);
        assertOffsets(works, of(0, 1, 2, 6));

        // fail some
        wm.failed(works.get(1));
        wm.failed(works.get(3));

        //
        works = wm.maybeGetWork(max);
        assertOffsets(works, of(8, 10));

        //
        advanceClockByDelay();

        //
        works = wm.maybeGetWork(max);
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
        assertThat(wm.maybeGetWork()).hasSize(1);
        assertThat(wm.maybeGetWork()).isEmpty();
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
    public void orderedByPartitionsParallel() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        registerSomeWork();

        var partition = 2;
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "66", "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "66", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "66", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        var works = wm.maybeGetWork();
        assertOffsets(works, of(0, 6));
        successAll(works);

        //
        works = wm.maybeGetWork();
        assertOffsets(works, of(1, 8));
        successAll(works);

        //
        works = wm.maybeGetWork();
        assertOffsets(works, of(2, 10));
        successAll(works);
    }

    private void successAll(List<WorkContainer<String, String>> works) {
        for (WorkContainer<String, String> work : works) {
            wm.success(work);
        }
    }

    @Test
    public void orderedByKeyParallel() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(KEY);

        registerSomeWork();

        var partition = 2;
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "key-a", "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "key-a", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "key-b", "value");
        var rec4 = new ConsumerRecord<>(INPUT_TOPIC, partition, 12, "key-c", "value");
        var rec5 = new ConsumerRecord<>(INPUT_TOPIC, partition, 15, "key-a", "value");
        var rec6 = new ConsumerRecord<>(INPUT_TOPIC, partition, 20, "key-c", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec2, rec3, rec, rec4, rec5, rec6));
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        var works = wm.maybeGetWork();
        works.sort(Comparator.naturalOrder()); // we actually don't care about the order
        assertOffsets(works, of(0, 6, 8, 12));
        successAll(works);

        //
        works = wm.maybeGetWork();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(1, 10, 20));
        successAll(works);

        //
        works = wm.maybeGetWork();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(2, 15));
        successAll(works);

        works = wm.maybeGetWork();
        assertOffsets(works, of());
    }

    @Test
    @Disabled
    public void unorderedPartitionsGreedy() {
    }

    //        @Test
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 10, 20, 30, 50, 1000})
    public void highVolumeKeyOrder(int quantity) {
        int uniqueKeys = 100;

        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupWorkManager(build);

        KafkaTestUtils ktu = new KafkaTestUtils(new MockConsumer(OffsetResetStrategy.EARLIEST));

        List<Integer> keys = range(uniqueKeys).list();

        var records = ktu.generateRecords(keys, quantity);
        var flattened = ktu.flatten(records.values());
        Collections.sort(flattened, (o1, o2) -> Long.compare(o1.offset(), o2.offset()));

        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, 0), flattened);
        var recs = new ConsumerRecords<>(m);

        //
        wm.registerWork(recs);

        //
        List<WorkContainer<String, String>> work = wm.maybeGetWork();

        //
        assertThat(work).hasSameSizeAs(records.keySet());
    }

    @Test
    public void treeMapOrderingCorrect() {
        KafkaTestUtils ktu = new KafkaTestUtils(new MockConsumer(OffsetResetStrategy.EARLIEST));

        int i = 10;
        var records = ktu.generateRecords(i);

        var treeMap = new TreeMap<Long, WorkContainer<String, String>>();
        for (ConsumerRecord<String, String> record : records) {
            treeMap.put(record.offset(), new WorkContainer<>(record));
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
        var work = wm.maybeGetWork();
        assertThat(work).hasSize(3);

        //
        for (var w : work) {
            w.onUserFunctionSuccess();
            wm.success(w);
        }

        //
        assertThat(wm.getWorkQueuedInShardsCount()).isZero();
        assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(3);

        // drain commit queue
        var completedFutureOffsets = wm.findCompletedEligibleOffsetsAndRemove();
        assertThat(completedFutureOffsets).hasSize(1); // coalesces (see log)
        assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(0);
    }

}

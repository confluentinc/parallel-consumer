package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.FakeRuntimeException;
import io.confluent.parallelconsumer.ManagedTruth;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.truth.CommitHistorySubject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.testcontainers.shaded.org.yaml.snakeyaml.tokens.Token.ID.Key;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Needs to run in {@link ExecutionMode#SAME_THREAD} because it manipulates the static state in
 * {@link WorkContainer#setStaticModule(PCModule)}.
 *
 * @see WorkManager
 */
@Execution(ExecutionMode.SAME_THREAD)
@Slf4j
public class WorkManagerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    WorkManager<String, String> wm;

    int offset;

    PCModuleTestEnv module;

    final static int minBatchSize = 5;
    final static int maxBatchSize = 7;

    @BeforeEach
    public void setup() {
        var options = ParallelConsumerOptions.builder().build();
        setupWorkManager(options);
    }

    private MutableClock getClock() {
        return module.getMutableClock();
    }

    protected List<WorkContainer<String, String>> successfulWork = new ArrayList<>();

    private void setupWorkManager(ParallelConsumerOptions options) {
        offset = 0;

        var mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var optsOverride = options.toBuilder().consumer(mockConsumer).build();

        module = new PCModuleTestEnv(optsOverride);

        wm = module.workManager();
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Heard some successful work: {}", work);
            successfulWork.add(work);
        });

        module.setWorkManager(wm);
    }

    private void assignPartition(final int partition) {
        wm.onPartitionsAssigned(UniLists.of(topicPartitionOf(partition)));
    }

    @NotNull
    private TopicPartition topicPartitionOf(int partition) {
        return new TopicPartition(INPUT_TOPIC, partition);
    }

    private void registerSomeWork() {
        registerSomeWork(0, 3, false);
    }

    /**
     * Adds 3 units of work
     */
    private void registerSomeWork(int partition, int numberOfRecords, boolean randomKey) {
        assignPartition(partition);

        List<ConsumerRecord<String, String>> records  = IntStream.rangeClosed(1, numberOfRecords).boxed()
                .map(i -> makeRec("" + i, getKey(randomKey), partition)).collect(Collectors.toList());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(topicPartitionOf(partition), records);
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(new EpochAndRecordsMap(recs, wm.getPm()));
    }

    @NotNull
    private static String getKey(boolean randomKey) {
        String random = randomKey ? "-" + UUID.randomUUID() : "";

        String key = "key-0" + random;
        return key;
    }

    private ConsumerRecord<String, String> makeRec(String value, String key, int partition) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, partition, offset, key, value);
        offset++;
        return stringStringConsumerRecord;
    }

    @ParameterizedTest
    @EnumSource
    void basic(ParallelConsumerOptions.ProcessingOrder order) {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(order)
                .build());
        registerSomeWork();

        //
        var gottenWork = wm.getWorkIfAvailable();

        if (order == UNORDERED) {
            assertThat(gottenWork).hasSize(3);
            assertOffsets(gottenWork, of(0, 1, 2));
        } else {
            assertThat(gottenWork).hasSize(1);
            assertOffsets(gottenWork, of(0));
        }

        //
        wm.onSuccessResult(gottenWork.get(0));

        //
        gottenWork = wm.getWorkIfAvailable();

        if (order == UNORDERED) {
            assertThat(gottenWork).isEmpty();
        } else {
            assertThat(gottenWork).hasSize(1);
            assertOffsets(gottenWork, of(1));
        }

        //
        gottenWork = wm.getWorkIfAvailable();
        assertThat(gottenWork).isEmpty();
    }

    @Test
    void testUnorderedAndDelayed() {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .build());
        registerSomeWork();

        int max = 2;

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertThat(workRetrieved).hasSize(2);
            assertOffsets(workRetrieved, of(0, 1));

            // pass first, fail second
            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
            WorkContainer<String, String> fail = workRetrieved.get(1);
            fail(fail);
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(2),
                    "no order restriction, 1's delay won't have passed - should get remaining in queue not yet failed");

            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(), "delay won't have passed so should not retrieve anything");

            advanceClockBySlightlyLessThanDelay();
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of());

            advanceClockByDelay();
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(1),
                    "should retrieve 1 given clock has been advanced and retry delay should be over");
            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
        }

        assertThat(successfulWork)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0, 2, 1));
    }


    @ParameterizedTest
    @MethodSource("workArgsProvider")
    void minBatchSizeTest(int numberOfRecords, int expected, ParallelConsumerOptions.ProcessingOrder order,
                          boolean isRandomKey) {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(order)
                .minBatchTimeoutInMillis(100)
                .minBatchSize(minBatchSize)
                .batchSize(maxBatchSize)
                .build());
        //add first 3
        registerSomeWork(0, numberOfRecords, isRandomKey);
        var gottenWork = wm.getWorkIfAvailable();
        assertThat(gottenWork).hasSize(expected);//not enough work

    }

    private static Stream<Arguments> workArgsProvider() {
        return Stream.of(
                Arguments.of(minBatchSize - 1, 0, UNORDERED, false),
                Arguments.of(minBatchSize - 1, 0, KEY, false),
                Arguments.of(minBatchSize - 1, 0, PARTITION, false),
                Arguments.of(minBatchSize, minBatchSize, UNORDERED, false),
                //Since in key and Partition there is no new data there will not be a new min batch
                Arguments.of(minBatchSize, 0, KEY, false),
                Arguments.of(minBatchSize, 0, PARTITION, false),
                //When key is random we expect KEY to be the same as UNORDERED
                Arguments.of(minBatchSize, minBatchSize, KEY, true),
                Arguments.of(maxBatchSize + 1, maxBatchSize, UNORDERED, false),
                Arguments.of(maxBatchSize + 1, maxBatchSize, KEY, true),
                Arguments.of(maxBatchSize * 3 +1 , maxBatchSize * 3, UNORDERED, false),
                Arguments.of(maxBatchSize * 3 +1 , maxBatchSize * 3, KEY, true),
                Arguments.of(maxBatchSize + minBatchSize, maxBatchSize + minBatchSize, UNORDERED, false),
                Arguments.of(maxBatchSize + minBatchSize, maxBatchSize + minBatchSize, KEY, true)
        );
    }


    private void succeed(WorkContainer<String, String> succeed) {
        succeed.onUserFunctionSuccess();
        wm.onSuccessResult(succeed);
    }

    private void succeed(Iterable<WorkContainer<String, String>> succeed) {
        succeed.forEach(this::succeed);
    }

    /**
     * Checks the offsets of the work, matches the offsets in the provided list
     *
     * @deprecated use {@link CommitHistorySubject} or similar instead
     */
    @Deprecated
    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>>
    assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected, String msg) {
        return assertThat(works)
                .as(msg)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(expected);
    }

    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>>
    assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected) {
        return assertOffsets(works, expected, "offsets of work given");
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

        succeed(w);

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
        fail(wc);

        // nothing available to get
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // advance clock to make delay pass
        advanceClockByDelay();

        // work should now be ready to take
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));

        wc = works.get(0);
        fail(wc);

        advanceClock(wc.getRetryDelayConfig().minus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // increased advance to allow for bigger delay under high load during parallel test execution.
        advanceClock(wc.getRetryDelayConfig().plus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));
        succeed(works.get(0));

        assertOffsets(successfulWork, of(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(1));
        succeed(works.get(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(2));
        succeed(works.get(0));

        // check all published in the end
        assertOffsets(successfulWork, of(0, 1, 2));
    }

    @Test
    void containerDelay() {
        var wc = new WorkContainer<String, String>(0, mock(ConsumerRecord.class), module);
        assertThat(wc.isDelayPassed()).isTrue(); // when new, there's no delay
        wc.onUserFunctionFailure(new FakeRuntimeException(""));
        assertThat(wc.isDelayPassed()).isFalse();
        advanceClockBySlightlyLessThanDelay();
        assertThat(wc.isDelayPassed()).isFalse();
        advanceClockByDelay();
        ManagedTruth.assertThat(wc).isDelayPassed();
    }

    private void advanceClockBySlightlyLessThanDelay() {
        Duration retryDelay = module.options().getDefaultMessageRetryDelay();
        Duration duration = retryDelay.dividedBy(2);
        getClock().add(duration);
    }

    private void advanceClockByDelay() {
        Duration retryDelay = module.options().getDefaultMessageRetryDelay();
        getClock().add(retryDelay);
    }

    private void advanceClock(Duration by) {
        getClock().add(by);
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
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

        int max = 10;

        var works = wm.getWorkIfAvailable(4);
        assertOffsets(works, of(0, 1, 2, 6));

        // fail some
        fail(works.get(1));
        fail(works.get(3));

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(8, 10));

        //
        advanceClockByDelay();

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1, 6));
    }

    private void registerWork(ConsumerRecords<String, String> recs) {
        wm.registerWork(new EpochAndRecordsMap<>(recs, wm.getPm()));
    }


    private void fail(WorkContainer<String, String> wc) {
        wc.onUserFunctionFailure(null);
        wm.onFailureResult(wc);
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

    public static class FluentQueue<T> implements Iterable<T> {
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
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

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
            wm.onSuccessResult(work);
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
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec0, rec4, rec5, rec6));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 10, 20, 30, 50, 1000})
    void highVolumeKeyOrder(int quantity) {
        int uniqueKeys = 100;

        var build = ParallelConsumerOptions.builder()
                .ordering(KEY)
                .build();
        setupWorkManager(build);

        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        List<Integer> keys = Range.listOfIntegers(uniqueKeys);

        var records = ktu.generateRecords(keys, quantity);
        var flattened = ktu.flatten(records.values());

        int partition = 0;
        var recs = new ConsumerRecords<>(UniMaps.of(topicPartitionOf(partition), flattened));

        assignPartition(partition);

        //
        registerWork(recs);

        //
        long awaiting = wm.getSm().getNumberOfWorkQueuedInShardsAwaitingSelection();
        assertThat(awaiting).isEqualTo(quantity);

        //
        List<WorkContainer<String, String>> work = wm.getWorkIfAvailable();

        //
        ManagedTruth.assertTruth(work).hasSameSizeAs(records);
    }

    @Test
    void treeMapOrderingCorrect() {
        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        int i = 10;
        var records = ktu.generateRecords(i);

        var treeMap = new TreeMap<Long, WorkContainer<String, String>>();
        for (ConsumerRecord<String, String> record : records) {
            treeMap.put(record.offset(), new WorkContainer<>(0, record, mock(PCModuleTestEnv.class)));
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
    void workQueuesEmptyWhenAllWorkComplete() {
        var build = ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .build();
        setupWorkManager(build);
        registerSomeWork();

        //
        var work = wm.getWorkIfAvailable();
        assertThat(work).hasSize(3);

        //
        succeed(work);

        //
        assertThat(wm.getSm().getNumberOfWorkQueuedInShardsAwaitingSelection()).isZero();
        assertThat(wm.getNumberOfIncompleteOffsets()).as("Partition commit queues are now empty").isZero();

        // drain commit queue
        var completedFutureOffsets = wm.collectCommitDataForDirtyPartitions();
        assertThat(completedFutureOffsets).hasSize(1); // coalesces (see log)
        var sync = completedFutureOffsets.values().stream().findFirst().get();
        Truth.assertThat(sync.offset()).isEqualTo(3);
        Truth.assertThat(sync.metadata()).isEmpty();
        PartitionState<String, String> state = wm.getPm().getPartitionState(topicPartitionOf(0));
        Truth.assertThat(state.getAllIncompleteOffsets()).isEmpty();
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
        m.put(topicPartitionOf(1), of(rec));
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, 2, 21, "21", "value");
        m.put(topicPartitionOf(2), of(rec2));
        var recs = new ConsumerRecords<>(m);
        registerWork(recs);

//        // force ingestion of records - see refactor: Queue unification #219
//        wm.tryToEnsureQuantityOfWorkQueuedAvailable(100);

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


    /**
     * Checks that when using shards are not starved when there's enough work queued to satisfy poll request from the
     * initial request (without needing to iterate to other shards)
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/236">#236</a> Under some conditions, a
     *         shard (by partition or key), can get starved for attention
     */
    @Test
    void starvation() {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(PARTITION)
                .build());

        registerSomeWork(0,3, false);
        registerSomeWork(1,3, false);
        registerSomeWork(2,3, false);

        var allWork = new ArrayList<WorkContainer<String, String>>();

        {
            var work = wm.getWorkIfAvailable(2);
            allWork.addAll(work);

            assertWithMessage("Should be able to get 2 records of work, one from each partition shard")
                    .that(work).hasSize(2);

            //
            var tpOne = work.get(0).getTopicPartition();
            var tpTwo = work.get(1).getTopicPartition();
            assertWithMessage("The partitions should be different")
                    .that(tpOne).isNotEqualTo(tpTwo);

        }

        {
            var work = wm.getWorkIfAvailable(2);
            assertWithMessage("Should be able to get only 1 more, from the third shard")
                    .that(work).hasSize(1);
            allWork.addAll(work);

            //
            var tpOne = work.get(0).getTopicPartition();
        }

        assertWithMessage("TPs all unique")
                .that(allWork.stream()
                        .map(WorkContainer::getTopicPartition)
                        .collect(Collectors.toList()))
                .containsNoDuplicates();

    }

}

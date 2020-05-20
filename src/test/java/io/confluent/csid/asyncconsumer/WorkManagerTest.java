package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.AdvancingWallClockProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.csid.asyncconsumer.WorkContainer.getRetryDelay;
import static java.time.Duration.ofSeconds;
import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WorkManagerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    WorkManager<String, String> wm;

    int offset = 0;

    Instant time = Instant.now();

    AdvancingWallClockProvider clock = new AdvancingWallClockProvider() {
        @Override
        public Instant getNow() {
            return time;
        }
    };

    @BeforeEach
    public void setup() {
        wm = new WorkManager<>();
        wm.setClock(clock);

        String key = "key";
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
    public void testRemoved() {
        boolean ordered = false;
        int max = 1;
        var works = wm.getWork(ordered, max);
        assertThat(works).hasSize(1);
        assertThat(works)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0));

        works = wm.getWork(ordered, max);
        assertThat(works).hasSize(1);
        assertWork(works, of(1));
    }

    @Test
    public void testUnorderedAndDelayed() {
        boolean ordered = false;
        int max = 2;

        var works = wm.getWork(ordered, max);
        assertThat(works).hasSize(2);
        assertWork(works, of(0, 1));

        wm.success(works.get(0));
        wm.failed(works.get(1));

        works = wm.getWork(ordered, max);
        assertWork(works, of(2));

        wm.success(works.get(0));

        works = wm.getWork(ordered, max);
        assertWork(works, of());

        advanceClockBySlightlyLessThanDelay();

        works = wm.getWork(ordered, max);
        assertWork(works, of());

        advanceClockByDelay();

        works = wm.getWork(ordered, max);
        assertWork(works, of(1));
        wm.success(works.get(0));

        assertThat(wm.getSuccessfulWork())
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0, 2, 1));
    }

    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>> assertWork(List<WorkContainer<String, String>> works, List<Integer> expected) {
        return assertThat(works)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(expected);
    }

    @Test
    public void testOrderedInFlightShouldBlockQueue() {
        boolean ordered = true;
        int max = 2;

        var works = wm.getWork(ordered, max);
        assertWork(works, of(0));
        var w = works.get(0);

        works = wm.getWork(ordered, max);
        assertWork(works, of()); // should be blocked by in flight

        wm.success(w);

        works = wm.getWork(ordered, max);
        assertWork(works, of(1));
    }

    @Test
    public void testOrderedAndDelayed() {
        boolean ordered = true;
        int max = 2;

        var works = wm.getWork(ordered, max);
        assertWork(works, of(0));
        var wc = works.get(0);
        wm.failed(wc);

        works = wm.getWork(ordered, max);
        assertWork(works, of());

        advanceClockByDelay();

        works = wm.getWork(ordered, max);
        assertWork(works, of(0));

        wc = works.get(0);
        wm.failed(wc);

        advanceClock(getRetryDelay().minus(ofSeconds(1)));

        works = wm.getWork(ordered, max);
        assertWork(works, of());

        advanceClock(getRetryDelay());

        works = wm.getWork(ordered, max);
        assertWork(works, of(0));
        wm.success(works.get(0));

        assertWork(wm.getSuccessfulWork(), of(0));

        works = wm.getWork(ordered, max);
        assertWork(works, of(1));
        wm.success(works.get(0));

        works = wm.getWork(ordered, max);
        assertWork(works, of(2));
        wm.success(works.get(0));

        // check all published in the end
        assertWork(wm.getSuccessfulWork(), of(0, 1, 2));
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
        String key = "key";
        int partition = 0;
        // mess with offset order for insertion
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, key, "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, key, "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, key, "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(INPUT_TOPIC, partition), of(rec, rec2, rec3));
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(recs);

        boolean ordered = true;
        int max = 10;

        var works = wm.getWork(ordered, max);
        assertWork(works, of(0));

        wm.failed(works.get(0));

        works = wm.getWork(false, max);
        assertWork(works, of(1, 2, 6, 8, 10));

        advanceClockByDelay();

        works = wm.getWork(false, max);
        assertWork(works, of(0));
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
    @Disabled
    public void maxOverall() {
    }

//    @Test
//    @Disabled
//    public void multipleFailures() {
//    }

    @Test
    @Disabled
    public void terminalFailureGoesToDlq() {
        assertThat(wm.getTerminallyFailedWork()).isNotNull();
    }
//
//    @Test
//    @Disabled
//    public void delayedOrdered() {
//    }
//
//    @Test
//    @Disabled
//    public void delayedUnordered() {
//    }

    @Test
    @Disabled
    public void orderedPartitionsParallel() {

    }

    @Test
    @Disabled
    public void unorderedPartitionsGreedy() {
    }
}

package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.util.Lists.list;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

@Tag("performance")
@Slf4j
class MultiInstanceHighVolumeTest extends BrokerIntegrationTest<String, String> {

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());

    public List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());

    public AtomicInteger processedCount = new AtomicInteger(0);

    public AtomicInteger producedCount = new AtomicInteger(0);

    int maxPoll = 500; // 500 is the kafka default

    CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_SYNC;

    ProcessingOrder order = ProcessingOrder.KEY;


    // todo multi commit mode, multi partition count, multi instance count? 2,3,10,100? more instances than partitions, more partitions than instances
    @SneakyThrows
    @Test
    void multiInstance() {
        numPartitions = 12;
        String inputTopicName = setupTopic(this.getClass().getSimpleName() + "-input");

//        int expectedMessageCount = 10_000_000;
        int expectedMessageCount = 30_000_00;
        log.info("Producing {} messages before starting test", expectedMessageCount);

        List<String> expectedKeys = getKcu().produceMessages(inputTopicName, expectedMessageCount);

        // setup
        ParallelEoSStreamProcessor<String, String> pcOne = buildPc(inputTopicName, maxPoll, order, commitMode);
        ParallelEoSStreamProcessor<String, String> pcTwo = buildPc(inputTopicName, maxPoll, order, commitMode);
        ParallelEoSStreamProcessor<String, String> pcThree = buildPc(inputTopicName, maxPoll, order, commitMode);

        // run
        var consumedByOne = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByTwo = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByThree = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        List<ProgressBar> bars = list();
        bars.add(run(expectedMessageCount / 3, pcOne, consumedByOne));
        bars.add(run(expectedMessageCount / 3, pcTwo, consumedByTwo));
        bars.add(run(expectedMessageCount / 3, pcThree, consumedByThree));

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time " +
                        "(expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(ofSeconds(60))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // .failFast( () -> pcThree.getFailureCause(), () -> pcThree.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .failFast("PC died - check logs", () -> pcThree.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(expectedMessageCount);

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());

        bars.forEach(ProgressBar::close);
    }

    private ParallelEoSStreamProcessor<String, String> buildPc(String inputTopicName, int maxPoll, ProcessingOrder order, CommitMode commitMode) {
        var pc = getKcu().buildPc(order, commitMode, maxPoll);
        pc.subscribe(of(inputTopicName));
        return pc;
    }

    Integer barId = 0;

    private ProgressBar run(final int expectedMessageCount, final ParallelEoSStreamProcessor<String, String> pc, List<ConsumerRecord<?, ?>> consumed) {
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        bar.setExtraMessage("#" + barId);
        pc.setMyId(Optional.of("id: " + barId));
        barId++;
        pc.poll(record -> {
                    processRecord(bar, record.getSingleConsumerRecord(), consumed);
                }
//                , consumeProduceResult -> {
//                    callBack(consumeProduceResult);
//                }
        );
        return bar;
    }

    @SneakyThrows
    private void processRecord(final ProgressBar bar,
                               final ConsumerRecord<String, String> record,
                               List<ConsumerRecord<?, ?>> consumed) {
//        try {
        // 1/5 chance of taking a long time
//        int chance = 10;
//        int dice = RandomUtils.nextInt(0, chance);
//        if (dice == 0) {
//            Thread.sleep(100);
//        } else {
//            Thread.sleep(RandomUtils.nextInt(3, 20));
//        }
        bar.stepBy(1);
        consumedKeys.add(record.key());
        processedCount.incrementAndGet();
        consumed.add(record);
//        return new ProducerRecord<>(outputName, record.key(), "data");
    }

}

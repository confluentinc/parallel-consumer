
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static one.util.streamex.StreamEx.of;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Originally created to investigate issue report #184
 */
@Slf4j
class MultiTopicTest extends BrokerIntegrationTest<String, String> {

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void multiTopic(ProcessingOrder order) {
        int numTopics = 3;
        List<NewTopic> multiTopics = getKcu().createTopics(numTopics);
        int recordsPerTopic = 1;
        multiTopics.forEach(singleTopic -> sendMessages(singleTopic, recordsPerTopic));

        var pc = getKcu().buildPc(order);
        pc.subscribe(of(multiTopics).map(NewTopic::name).toList());

        AtomicInteger messageProcessedCount = new AtomicInteger();

        pc.poll(pollContext -> {
            log.debug(pollContext.toString());
            messageProcessedCount.incrementAndGet();
        });

        // processed
        int expectedMessagesCount = recordsPerTopic * numTopics;
        await().untilAtomic(messageProcessedCount, Matchers.is(equalTo(expectedMessagesCount)));

        // commits
//        await().untilAsserted(() -> {
//            multiTopics.forEach(singleTopic -> assertCommit(singleTopic, recordsPerTopic));
//        });
    }


    @SneakyThrows
    private void sendMessages(NewTopic newTopic, int recordsPerTopic) {
        getKcu().produceMessages(newTopic.name(), recordsPerTopic);
    }

    private void assertCommit(NewTopic newTopic, int recordsPerTopic) {
        assertThat(getKcu().getLastConsumerConstructed())
                .hasCommittedToPartition(newTopic)
                .offset(recordsPerTopic);
    }

}

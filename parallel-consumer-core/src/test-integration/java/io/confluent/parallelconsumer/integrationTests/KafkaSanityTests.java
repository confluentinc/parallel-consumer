
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.Set;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class KafkaSanityTests extends KafkaTest<String, String> {

    /**
     * @link io.confluent.csid.asyncconsumer.BrokerPollSystem#pollBrokerForRecords
     */
    @Timeout(value = 20) // includes docker broker startup time, can be slow on CI machines
    @Test
    public void pausedConsumerStillLongPollsForNothing() {
        log.info("Setup topic");
        setupTopic();
        KafkaConsumer<String, String> c = kcu.consumer;
        log.info("Subscribe to topic");
        c.subscribe(UniLists.of(topic));
        Set<TopicPartition> assignment = c.assignment();
        log.info("Pause subscription");
        c.pause(assignment);

        log.info("Initial poll that can trigger some actions and take longer than expected");
        c.poll(ofSeconds(0));

        log.info("Second poll which is measured");
        Duration longPollTime = ofSeconds(1);
        Duration time = time(() -> {
            c.poll(longPollTime);
        });

        log.info("Poll blocked my thread for {}, hopefully slightly longer than {}", time, longPollTime);
        String desc = "Even though the consumer is paused ALL it's subscribed partitions, it will still perform a long poll against the server";
        var timePlusFluctuation = time.plus(ofMillis(100));
        assertThat(timePlusFluctuation).as(desc)
                .isGreaterThan(longPollTime);
    }

}

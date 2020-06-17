package io.confluent.csid.asyncconsumer.integrationTests;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class KafkaSanityTests extends KafkaTest<String, String> {

    /**
     * @see io.confluent.csid.asyncconsumer.BrokerPollSystem#pollBrokerForRecords
     */
    @Timeout(value = 5)
    @Test
    public void pausedConsumerStillLongPollsForNothing() {
        setupTopic();
        KafkaConsumer<String, String> c = kcu.consumer;
        c.subscribe(List.of(topic));
        Set<TopicPartition> assignment = c.assignment();
        c.pause(assignment);
        Duration longPollTime = ofSeconds(1);
        Duration time = time(() -> {
            c.poll(longPollTime);
        });
        log.debug("Poll blocked my thread for {}, hopefully longer than {}", time, longPollTime);
        String desc = "Even though the consumer is paused ALL it's subscribed partitions, it will still perform a long poll against the server";
        var timePlusFluctuation = time.plus(ofMillis(100));
        assertThat(timePlusFluctuation).as(desc)
                .isGreaterThan(longPollTime);
    }

}

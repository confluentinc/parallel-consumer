package io.confluent.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//@Isolated
@Slf4j
public class KafkaSanityTests extends BrokerIntegrationTest<String, String> {

    /**
     * @link io.confluent.csid.asyncconsumer.BrokerPollSystem#pollBrokerForRecords
     */
    @Timeout(value = 20) // includes docker broker startup time, can be slow on CI machines
    @Test
    public void pausedConsumerStillLongPollsForNothing() {
        log.info("Setup topic");
        setupTopic();
        KafkaConsumer<String, String> consumer = kcu.getConsumer();
        log.info("Subscribe to topic");
        consumer.subscribe(UniLists.of(topic));
        Set<TopicPartition> assignment = consumer.assignment();
        log.info("Pause subscription");
        consumer.pause(assignment);

        log.info("Initial poll that can trigger some actions and take longer than expected");
        consumer.poll(ofSeconds(0));

        log.info("Second poll which is measured");
        Duration longPollTime = ofSeconds(1);
        Duration time = time(() -> {
            consumer.poll(longPollTime);
        });

        log.info("Poll blocked my thread for {}, hopefully slightly longer than {}", time, longPollTime);
        String desc = "Even though the consumer is paused ALL it's subscribed partitions, it will still perform a long poll against the server";
        var timePlusFluctuation = time.plus(ofMillis(100));
        assertThat(timePlusFluctuation).as(desc)
                .isGreaterThan(longPollTime);
    }

    /**
     * Test our understanding of the offset metadata payload system - is the DefaultMaxMetadataSize available for each
     * partition, or to the total of all partitions in the commit?
     */
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
    // depends on OffsetMapCodecManager#DefaultMaxMetadataSize
    // todo remove static dependencies
    @Test
    void offsetMetadataSpaceAvailable() {
        numPartitions = 5;
        setupTopic();

        int maxCapacity = OffsetMapCodecManager.DefaultMaxMetadataSize;
        assertThat(maxCapacity)
                .as("approximate sanity - ensure start state settings (shared static state :`( )")
                .isGreaterThan(3000);

        KafkaConsumer<String, String> consumer = kcu.getConsumer();
        TopicPartition tpOne = new TopicPartition(topic, 0);
        TopicPartition tpTwo = new TopicPartition(topic, 1);
        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();

        String payload = RandomStringUtils.randomAlphanumeric(maxCapacity);

        // fit the max
        {
            map.put(tpOne, new OffsetAndMetadata(0, payload));

            consumer.commitSync(map);
        }

        // fit just one more for one
        {
            map.put(tpOne, new OffsetAndMetadata(0, payload + "!"));
            assertThatThrownBy(() -> consumer.commitSync(map))
                    .isInstanceOf(OffsetMetadataTooLarge.class)
                    .hasMessageContainingAll("metadata", "offset request", "too large");
        }

        // fit double for one
        {
            map.put(tpOne, new OffsetAndMetadata(0, payload + payload));

            assertThatThrownBy(() -> consumer.commitSync(map))
                    .isInstanceOf(OffsetMetadataTooLarge.class)
                    .hasMessageContainingAll("metadata", "offset request", "too large");
        }

        // fit max for two
        {
            map.put(tpOne, new OffsetAndMetadata(0, payload));
            map.put(tpTwo, new OffsetAndMetadata(0, payload));

            consumer.commitSync(map);
        }

        // fit max for five - upper range
        {
            map.put(tpOne, new OffsetAndMetadata(0, payload));
            map.put(tpTwo, new OffsetAndMetadata(0, payload));
            map.put(new TopicPartition(topic, 2), new OffsetAndMetadata(0, payload));
            map.put(new TopicPartition(topic, 3), new OffsetAndMetadata(0, payload));
            map.put(new TopicPartition(topic, 4), new OffsetAndMetadata(0, payload));

            consumer.commitSync(map);
        }

        // go over for on one of the two
        {
            // fit double for one
            {
                map.put(tpOne, new OffsetAndMetadata(0, payload));
                map.put(tpTwo, new OffsetAndMetadata(0, payload + payload));
                assertThatThrownBy(() -> consumer.commitSync(map))
                        .isInstanceOf(OffsetMetadataTooLarge.class)
                        .hasMessageContainingAll("metadata", "offset request", "too large");
            }
        }
    }
}

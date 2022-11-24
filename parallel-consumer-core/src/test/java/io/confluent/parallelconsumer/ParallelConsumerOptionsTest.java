package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Check that various validation and combinations of {@link ParallelConsumerOptions} works.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions
 */
@Tag("transactions")
@Tag("#355")
class ParallelConsumerOptionsTest {

    @Test
    void testProducerRequired() {
        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                        .build()
                        .validate());


        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .allowEagerProcessingDuringTransactionCommit(true)
                        .build()
                        .validate());
    }

    /**
     * Test the deprecation phase of commit frequency
     */
    @Test
    void setTimeBetweenCommits() {
        var newFreq = Duration.ofMillis(100);
        var options = ParallelConsumerOptions.<String, String>builder()
                .commitInterval(newFreq)
                .consumer(new LongPollingMockConsumer<>(EARLIEST))
                .build();

        //
        assertThat(options.getCommitInterval()).isEqualTo(newFreq);

        //
        var pc = new ParallelEoSStreamProcessor<>(options);

        //
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(newFreq);

        //
        var testFreq = Duration.ofMillis(9);
        pc.setTimeBetweenCommits(testFreq);

        //
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(testFreq);
        assertThat(options.getCommitInterval()).isEqualTo(testFreq);
    }

    @Test
    void retrySettings() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .retrySettings(ParallelConsumerOptions.RetrySettings.builder()
                        .maxRetries(1)
                        .build())
                .consumer(new LongPollingMockConsumer<>(EARLIEST))
                .build();

        assertThrows(IllegalArgumentException.class, options::validate);
    }

}
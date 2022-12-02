package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.junit.jupiter.api.Tag;

/**
 * Check that various validation and combinations of {@link ParallelConsumerOptions} works.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions
 */
@Tag("transactions")
@Tag("#355")
class ParallelConsumerOptionsTest {

    // todo deprecated
//    /**
//     * Test the deprecation phase of commit frequency
//     */
//    @Test
//    void setTimeBetweenCommits() {
//        var newFreq = Duration.ofMillis(100);
//        var options = ParallelConsumerOptions.<String, String>builder()
//                .commitInterval(newFreq)
//                .consumer(new LongPollingMockConsumer<>(EARLIEST))
//                .build();
//
//        //
//        assertThat(options.getCommitInterval()).isEqualTo(newFreq);
//
//        //
//        var pc = new ParallelEoSStreamProcessor<>(options);
//
//        //
//        assertThat(pc.getTimeBetweenCommits()).isEqualTo(newFreq);
//
//        //
//        var testFreq = Duration.ofMillis(9);
//        pc.setTimeBetweenCommits(testFreq);
//
//        //
//        assertThat(pc.getTimeBetweenCommits()).isEqualTo(testFreq);
//        assertThat(options.getCommitInterval()).isEqualTo(testFreq);
//    }
}
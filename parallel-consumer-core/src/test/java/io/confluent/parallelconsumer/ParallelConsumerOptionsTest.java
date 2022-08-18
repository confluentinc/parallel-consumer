package io.confluent.parallelconsumer;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
class ParallelConsumerOptionsTest {

    /**
     * Test the deprecation phase of commit frequency
     */
    @Test
    void setTimeBetweenCommits() {
        var newFreq = Duration.ofMillis(100);
        var options = ParallelConsumerOptions.<String, String>builder().timeBetweenCommits(newFreq).build();

        //
        assertThat(options.getTimeBetweenCommits()).isEqualTo(newFreq);

        //
        var pc = new ParallelEoSStreamProcessor<>(options);

        //
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(newFreq);

        //
        var testFreq = Duration.ofMillis(9);
        pc.setTimeBetweenCommits(testFreq);
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(testFreq);
        assertThat(options.getTimeBetweenCommits()).isEqualTo(testFreq);
    }
}
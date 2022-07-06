package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.state.PartitionState;
import org.junit.Test;

/**
 * Tests the system when the input topic has transaction markers in its partitions.
 *
 * @author Antony Stubbs
 * @see PartitionState#getOffsetHighestSequentialSucceeded()
 * @see OffsetSimultaneousEncoder
 */
public class TransactionMarkersTest extends BrokerIntegrationTest<String, String> {

    /**
     * Originally written to test issue# XXX - that committing can happen successfully when the base offset for the
     * commit is adjacent to transaction markers in the input partitions.
     */
    @Test
    void single() {
    }

    /**
     * @see #single()
     */
    @Test
    void several() {
    }
}

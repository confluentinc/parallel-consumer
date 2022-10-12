package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;

import static io.confluent.csid.utils.JavaUtils.toTreeSet;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static org.assertj.core.api.Assertions.assertThat;

class BitSetEncodingTest {

    @SneakyThrows
    @Test
    void basic() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet());
        var completes = UniLists.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);
        int length = 11;
        BitSetEncoder bs = new BitSetEncoder(length, offsetSimultaneousEncoder, v2);

        bs.encodeIncompleteOffset(0);
        bs.encodeCompletedOffset(1);
        bs.encodeCompletedOffset(2);
        bs.encodeCompletedOffset(3);
        bs.encodeIncompleteOffset(4);
        bs.encodeCompletedOffset(5);
        bs.encodeIncompleteOffset(6);
        bs.encodeIncompleteOffset(7);
        bs.encodeIncompleteOffset(8);
        bs.encodeCompletedOffset(9);
        bs.encodeIncompleteOffset(10);

        // before serialisation
        {
            assertThat(bs.getBitSet().stream().toArray()).containsExactly(1, 2, 3, 5, 9);
        }

        // after serialisation
        {
            byte[] raw = bs.serialise();

            byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.BitSetV2, ByteBuffer.wrap(raw)));

            OffsetMapCodecManager.HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

            assertThat(result.getHighestSeenOffset()).contains(10L);

            assertThat(result.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);
        }
    }
}

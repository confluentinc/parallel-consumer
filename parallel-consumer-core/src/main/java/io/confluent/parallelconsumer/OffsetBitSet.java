package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import static io.confluent.csid.utils.Range.range;

/**
 * Deserialisation tools for {@link BitsetEncoder}.
 * <p>
 * todo unify or refactor with {@link BitsetEncoder}. Why was it ever seperate?
 *
 * @see BitsetEncoder
 */
@Slf4j
public class OffsetBitSet {

    static String deserialiseBitSetWrap(ByteBuffer wrap, OffsetEncoding.Version version) {
        wrap.rewind();

        int originalBitsetSize = switch (version) {
            case v1 -> (int) wrap.getShort(); // up cast ok
            case v2 -> wrap.getInt();
        };

        ByteBuffer slice = wrap.slice();
        return deserialiseBitSet(originalBitsetSize, slice);
    }

    static String deserialiseBitSet(int originalBitsetSize, ByteBuffer s) {
        BitSet bitSet = BitSet.valueOf(s);

        StringBuilder result = new StringBuilder(bitSet.size());
        for (var offset : range(originalBitsetSize)) {
            if (bitSet.get(offset)) {
                result.append('x');
            } else {
                result.append('o');
            }
        }

        return result.toString();
    }

    static Tuple<Long, Set<Long>> deserialiseBitSetWrapToIncompletes(OffsetEncoding encoding, long baseOffset, ByteBuffer wrap) {
        wrap.rewind();
        int originalBitsetSize = switch (encoding) {
            case BitSet -> wrap.getShort();
            case BitSetV2 -> wrap.getInt();
            default -> throw new InternalRuntimeError("Invalid state");
        };
        ByteBuffer slice = wrap.slice();
        Set<Long> incompletes = deserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
        long highestSeenRecord = baseOffset + originalBitsetSize;
        return Tuple.pairOf(highestSeenRecord, incompletes);
    }

    static Set<Long> deserialiseBitSetToIncompletes(long baseOffset, int originalBitsetSize, ByteBuffer inputBuffer) {
        BitSet bitSetOfSucceededRecords = BitSet.valueOf(inputBuffer);
        var incompletes = new HashSet<Long>(); // can't know how big this needs to be yet
        for (var relativeOffset : range(originalBitsetSize)) {
            long offset = baseOffset + relativeOffset;
            if (bitSetOfSucceededRecords.get(relativeOffset)) {
                log.trace("Ignoring completed offset relative {} offset {}", relativeOffset, offset);
            } else {
                incompletes.add(offset);
            }
        }
        return incompletes;
    }
}

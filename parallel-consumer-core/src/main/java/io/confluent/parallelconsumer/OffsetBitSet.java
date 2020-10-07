package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import static io.confluent.csid.utils.Range.range;

@Slf4j
public class OffsetBitSet {

    static String deserialiseBitSetWrap(ByteBuffer wrap) {
        wrap.rewind();
        short originalBitsetSize = wrap.getShort();
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

    static ParallelConsumer.Tuple<Long, Set<Long>> deserialiseBitSetWrapToIncompletes(long baseOffset, ByteBuffer wrap) {
        wrap.rewind();
        short originalBitsetSize = wrap.getShort();
        ByteBuffer slice = wrap.slice();
        Set<Long> incompletes = deserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
        long highwaterMark = baseOffset + originalBitsetSize;
        return ParallelConsumer.Tuple.pairOf(highwaterMark, incompletes);
    }

    static Set<Long> deserialiseBitSetToIncompletes(long baseOffset, int originalBitsetSize, ByteBuffer inputBuffer) {
        BitSet bitSet = BitSet.valueOf(inputBuffer);
        var incompletes = new HashSet<Long>(1); // can't know how big this needs to be yet
        for (var relativeOffset : range(originalBitsetSize)) {
            long offset = baseOffset + relativeOffset;
            if (bitSet.get(relativeOffset)) {
                log.trace("Ignoring completed offset");
            } else {
                incompletes.add(offset);
            }
        }
        return incompletes;
    }
}

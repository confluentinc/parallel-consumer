package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import static io.confluent.csid.utils.Range.range;

/**
 * Deserialisation tools for {@link BitSetEncoder}.
 * <p>
 * todo unify or refactor with {@link BitSetEncoder}. Why was it ever seperate?
 *
 * @see BitSetEncoder
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

    static HighestOffsetAndIncompletes deserialiseBitSetWrapToIncompletes(OffsetEncoding encoding, long baseOffset, ByteBuffer wrap) {
        wrap.rewind();
        int originalBitsetSize = switch (encoding) {
            case BitSet -> wrap.getShort();
            case BitSetV2 -> wrap.getInt();
            default -> throw new InternalRuntimeError("Invalid state");
        };
        ByteBuffer slice = wrap.slice();
        Set<Long> incompletes = deserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
        long highestSeenOffset = baseOffset + originalBitsetSize - 1;
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

    static Set<Long> deserialiseBitSetToIncompletes(long baseOffset, int originalBitsetSize, ByteBuffer inputBuffer) {
        BitSet bitSet = BitSet.valueOf(inputBuffer);
        int numberOfIncompletes = originalBitsetSize - bitSet.cardinality();
        var incompletes = new HashSet<Long>(numberOfIncompletes);
        for (var relativeOffset : range(originalBitsetSize)) {
            long offset = baseOffset + relativeOffset;
            if (bitSet.get(relativeOffset)) {
                log.trace("Ignoring completed offset {}", relativeOffset);
            } else {
                incompletes.add(offset);
            }
        }
        return incompletes;
    }
}

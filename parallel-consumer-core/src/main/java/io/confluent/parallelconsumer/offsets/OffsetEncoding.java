package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.*;

/**
 * todo javadoc
 *
 * @author Antony Stubbs
 */
@ToString
@RequiredArgsConstructor
public enum OffsetEncoding {
    ByteArray(v1, (byte) 'L'),
    ByteArrayCompressed(v1, (byte) 'î'),
    BitSet(v1, (byte) 'l'),
    BitSetCompressed(v1, (byte) 'a'),
    RunLength(v1, (byte) 'n'),
    RunLengthCompressed(v1, (byte) 'J'),

    /**
     * switch from encoding bitset length as a short to an integer (length of 32,000 was reasonable too short)
     */
    BitSetV2(v2, (byte) 'o'),
    BitSetV2Compressed(v2, (byte) 's'),

    /**
     * switch from encoding run lengths as Shorts to Integers
     */
    RunLengthV2(v2, (byte) 'e'),
    RunLengthV2Compressed(v2, (byte) 'p'),

    /**
     * switch from encoding run lengths as Integers to Longs to support VERY long continuous run lengths
     */
    RunLengthV3(v3, (byte) 'h'),
    RunLengthV3Compressed(v3, (byte) 'i');

    public enum Version {
        v1, v2, v3
    }

    public final Version version;

    @Getter
    public final byte magicByte;

    private static final Map<Byte, OffsetEncoding> magicMap = Arrays.stream(values()).collect(Collectors.toMap(OffsetEncoding::getMagicByte, Function.identity()));

    public static OffsetEncoding decode(byte magic) {
        OffsetEncoding encoding = magicMap.get(magic);
        if (encoding == null) {
            throw new RuntimeException("Unexpected magic: " + magic);
        } else {
            return encoding;
        }
    }

    public String description() {
        return name() + ":" + version;
    }
}

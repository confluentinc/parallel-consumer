package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ToString
@RequiredArgsConstructor
enum OffsetEncoding {
    ByteArray((byte) 'L'),
    ByteArrayCompressed((byte) 'î'),
    BitSet((byte) 'l'),
    BitSetCompressed((byte) 'a'),
    RunLength((byte) 'n'),
    RunLengthCompressed((byte) 'J');

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
}

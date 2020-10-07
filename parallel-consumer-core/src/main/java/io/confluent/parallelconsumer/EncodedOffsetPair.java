package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerImpl.Tuple;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Set;

import static io.confluent.parallelconsumer.OffsetBitSet.deserialiseBitSetWrap;
import static io.confluent.parallelconsumer.OffsetBitSet.deserialiseBitSetWrapToIncompletes;
import static io.confluent.parallelconsumer.OffsetRunLength.*;
import static io.confluent.parallelconsumer.OffsetSimpleSerialisation.decompressZstd;
import static io.confluent.parallelconsumer.OffsetSimpleSerialisation.deserialiseByteArrayToBitMapString;

/**
 * @see #unwrap
 */
@Slf4j
final class EncodedOffsetPair implements Comparable<EncodedOffsetPair> {

    @Getter
    OffsetEncoding encoding;
    @Getter
    ByteBuffer data;

    /**
     * @see #unwrap
     */
    EncodedOffsetPair(OffsetEncoding encoding, ByteBuffer data) {
        this.encoding = encoding;
        this.data = data;
    }

    @Override
    public int compareTo(EncodedOffsetPair o) {
        return Integer.compare(data.capacity(), o.getData().capacity());
    }

    /**
     * Used for printing out the comparative map of each encoder
     */
    @Override
    public String toString() {
        return "\n{" + encoding.name() + ", \t\t\tsize=" + data.capacity() + "}";
    }

    /**
     * Copies array out of the ByteBuffer
     */
    public byte[] readDataArrayForDebug() {
        return copyBytesOutOfBufferForDebug(data);
    }

    private static byte[] copyBytesOutOfBufferForDebug(ByteBuffer bbData) {
        bbData.position(0);
        byte[] bytes = new byte[bbData.remaining()];
        bbData.get(bytes, 0, bbData.limit());
        return bytes;
    }

    static EncodedOffsetPair unwrap(byte[] input) {
        ByteBuffer wrap = ByteBuffer.wrap(input).asReadOnlyBuffer();
        byte magic = wrap.get();
        OffsetEncoding decode = OffsetEncoding.decode(magic);
        ByteBuffer slice = wrap.slice();

        return new EncodedOffsetPair(decode, slice);
    }

    @SneakyThrows
    public String getDecodedString() {
        String binaryArrayString = switch (encoding) {
            case ByteArray -> deserialiseByteArrayToBitMapString(data);
            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrap(data);
            case BitSetCompressed -> deserialiseBitSetWrap(decompressZstd(data));
            case RunLength -> runLengthDecodeToString(runLengthDeserialise(data));
            case RunLengthCompressed -> runLengthDecodeToString(runLengthDeserialise(decompressZstd(data)));
        };
        return binaryArrayString;
    }

    @SneakyThrows
    public Tuple<Long, Set<Long>> getDecodedIncompletes(long baseOffset) {
        Tuple<Long, Set<Long>> binaryArrayString = switch (encoding) {
//            case ByteArray -> deserialiseByteArrayToBitMapString(data);
//            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrapToIncompletes(baseOffset, data);
            case BitSetCompressed -> deserialiseBitSetWrapToIncompletes(baseOffset, decompressZstd(data));
            case RunLength -> runLengthDecodeToIncompletes(baseOffset, data);
            case RunLengthCompressed -> runLengthDecodeToIncompletes(baseOffset, decompressZstd(data));
            default -> throw new UnsupportedOperationException("Encoding (" + encoding.name() + ") not supported");
        };
        return binaryArrayString;
    }
}

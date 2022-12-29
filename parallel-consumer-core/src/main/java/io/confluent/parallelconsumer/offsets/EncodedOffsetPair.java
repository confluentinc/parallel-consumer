package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static io.confluent.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrap;
import static io.confluent.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrapToIncompletes;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.offsets.OffsetRunLength.*;
import static io.confluent.parallelconsumer.offsets.OffsetSimpleSerialisation.decompressZstd;
import static io.confluent.parallelconsumer.offsets.OffsetSimpleSerialisation.deserialiseByteArrayToBitMapString;

/**
 * Encapsulates the encoding type, and the actual encoded data, when creating an offset map encoding. Central place for
 * decoding  the data.
 *
 * @author Antony Stubbs
 * @see #unwrap
 */
@Slf4j
public final class EncodedOffsetPair implements Comparable<EncodedOffsetPair> {

    public static final Comparator<EncodedOffsetPair> SIZE_COMPARATOR = Comparator.comparingInt(x -> x.data.capacity());
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
        return SIZE_COMPARATOR.compare(this, o);
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
        OffsetEncoding decode = decode(magic);
        ByteBuffer slice = wrap.slice();

        return new EncodedOffsetPair(decode, slice);
    }

    @SneakyThrows
    public String getDecodedString() {
        String binaryArrayString = switch (encoding) {
            case ByteArray -> deserialiseByteArrayToBitMapString(data);
            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrap(data, v1);
            case BitSetCompressed -> deserialiseBitSetWrap(decompressZstd(data), v1);
            case RunLength -> runLengthDecodeToString(runLengthDeserialise(data));
            case RunLengthCompressed -> runLengthDecodeToString(runLengthDeserialise(decompressZstd(data)));
            case BitSetV2 -> deserialiseBitSetWrap(data, v2);
            case BitSetV2Compressed -> deserialiseBitSetWrap(data, v2);
            case RunLengthV2 -> deserialiseBitSetWrap(data, v2);
            case RunLengthV2Compressed -> deserialiseBitSetWrap(data, v2);
            default ->
                    throw new InternalRuntimeException("Invalid state"); // todo why is this needed? what's not covered?
        };
        return binaryArrayString;
    }

    @SneakyThrows
    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset) {
        HighestOffsetAndIncompletes binaryArrayString = switch (encoding) {
//            case ByteArray -> deserialiseByteArrayToBitMapString(data);
//            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetCompressed -> deserialiseBitSetWrapToIncompletes(BitSet, baseOffset, decompressZstd(data));
            case RunLength -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthCompressed -> runLengthDecodeToIncompletes(RunLength, baseOffset, decompressZstd(data));
            case BitSetV2 -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetV2Compressed -> deserialiseBitSetWrapToIncompletes(BitSetV2, baseOffset, decompressZstd(data));
            case RunLengthV2 -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthV2Compressed -> runLengthDecodeToIncompletes(RunLengthV2, baseOffset, decompressZstd(data));
            case KafkaStreams, KafkaStreamsV2 ->
                    throw new KafkaStreamsEncodingNotSupported();
            default ->
                    throw new UnsupportedOperationException("Encoding (" + encoding.description() + ") not supported");
        };
        return binaryArrayString;
    }
}

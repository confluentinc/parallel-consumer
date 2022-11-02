package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.ToString;

import java.nio.ByteBuffer;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArrayCompressed;

/**
 * Encodes offsets into a {@link ByteBuffer}. Doesn't have any advantage over  the {@link BitSetEncoder} and
 * {@link RunLengthEncoder}, but can be useful for testing and comparison.
 *
 * @author Antony Stubbs
 */
@ToString(callSuper = true)
public class ByteBufferEncoder extends OffsetEncoder {

    private final ByteBuffer bytesBuffer;

    public ByteBufferEncoder(long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        super(offsetSimultaneousEncoder, OffsetEncoding.Version.v1);

        // safe cast the length to an int, as we're not expecting to have more than 2^31 offsets
        final int safeCast = Math.toIntExact(length);
        // safe cast the length to an int, as we're not expecting to have more than 2^31 offsets
        final int safeCast = Math.toIntExact(length);
        this.bytesBuffer = ByteBuffer.allocate(1 + safeCast);
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return ByteArray;
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return ByteArrayCompressed;
    }

    @Override
    public void encodeIncompleteOffset(final long relativeOffset) {
        this.bytesBuffer.put((byte) 0);
    }

    @Override
    public void encodeCompletedOffset(final long relativeOffset) {
        this.bytesBuffer.put((byte) 1);
    }

    @Override
    public byte[] serialise() {
        return this.bytesBuffer.array();
    }

    @Override
    public int getEncodedSize() {
        return this.bytesBuffer.capacity();
    }

    @Override
    protected byte[] getEncodedBytes() {
        return this.bytesBuffer.array();
    }

}

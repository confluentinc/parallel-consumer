package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.nio.ByteBuffer;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArrayCompressed;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class ByteBufferEncoder extends OffsetEncoder {

    private final ByteBuffer bytesBuffer;

    public ByteBufferEncoder(long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        super(offsetSimultaneousEncoder);
        this.bytesBuffer = ByteBuffer.allocate(1 + length);
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
    public void encodeIncompleteOffset(final int rangeIndex) {
        this.bytesBuffer.put((byte) 0);
    }

    @Override
    public void encodeCompletedOffset(final int rangeIndex) {
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

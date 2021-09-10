package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
import java.nio.ByteBuffer;

import static io.confluent.parallelconsumer.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.OffsetEncoding.ByteArrayCompressed;

class ByteBufferEncoder extends OffsetEncoder {

    private final ByteBuffer bytesBuffer;

    public ByteBufferEncoder(final int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
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

package io.confluent.parallelconsumer;

import java.nio.ByteBuffer;

import static io.confluent.parallelconsumer.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.OffsetEncoding.ByteArrayCompressed;

class ByteBufferEncoder extends OffsetEncoderBase {

    private final ByteBuffer bytesBuffer;

    public ByteBufferEncoder(final long baseOffset, final int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        super(baseOffset, offsetSimultaneousEncoder);
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
    public void encodeIncompleteOffset(final long newBaseOffset, final int relativeOffset) {
        this.bytesBuffer.put((byte) 0);
    }

    @Override
    public void encodeCompletedOffset(final long newBaseOffset, final int relativeOffset) {
        this.bytesBuffer.put((byte) 1);
    }

    @Override
    public byte[] serialise() {
        return this.bytesBuffer.array();
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//sdf
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//sdf
//    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {

    }

    @Override
    public void encodeCompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {

    }

    @Override
    public int getEncodedSize() {
        return this.bytesBuffer.capacity();
    }

    @Override
    public int getEncodedSizeEstimate() {
        return this.bytesBuffer.capacity();
    }

    @Override
    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) {
        throw new InternalRuntimeError("Na");
    }

    @Override
    public byte[] getEncodedBytes() {
        return this.bytesBuffer.array();
    }

}

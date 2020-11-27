package io.confluent.parallelconsumer;

import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class OffsetEncoder {

    private final OffsetSimultaneousEncoder offsetSimultaneousEncoder;

    public OffsetEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        this.offsetSimultaneousEncoder = offsetSimultaneousEncoder;
    }

    protected abstract OffsetEncoding getEncodingType();

    protected abstract OffsetEncoding getEncodingTypeCompressed();

    abstract void containsIndex(final int rangeIndex);

    abstract void doesNotContainIndex(final int rangeIndex);

    abstract byte[] serialise();

    abstract int getEncodedSize();

    boolean quiteSmall() {
        return this.getEncodedSize() < OffsetSimultaneousEncoder.LARGE_INPUT_MAP_SIZE_THRESHOLD;
    }

    byte[] compress() throws IOException {
        return OffsetSimpleSerialisation.compressZstd(this.getEncodedBytes());
    }

    final void register() {
        final byte[] bytes = this.serialise();
        final OffsetEncoding encodingType = this.getEncodingType();
        this.register(encodingType, bytes);
    }

    private void register(final OffsetEncoding type, final byte[] bytes) {
        offsetSimultaneousEncoder.sortedEncodings.add(new EncodedOffsetPair(type, ByteBuffer.wrap(bytes)));
        offsetSimultaneousEncoder.encodingMap.put(type, bytes);
    }

    @SneakyThrows
    void registerCompressed() {
        final byte[] compressed = compress();
        final OffsetEncoding encodingType = this.getEncodingTypeCompressed();
        this.register(encodingType, compressed);
    }

    protected abstract byte[] getEncodedBytes();
}

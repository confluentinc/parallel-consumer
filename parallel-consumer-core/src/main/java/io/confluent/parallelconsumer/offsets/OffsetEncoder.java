package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Base OffsetEncoder, defining the contract for encoding offset data.
 *
 * @author Antony Stubbs
 */
// metrics: avg offsets mapped per bit, average encoded size, avg time to encode,
@ToString
@Slf4j
public abstract class OffsetEncoder {

    /**
     * Implementation version of the encoding
     */
    protected final OffsetEncoding.Version version;

    private final OffsetSimultaneousEncoder offsetSimultaneousEncoder;

    protected OffsetEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, OffsetEncoding.Version version) {
        this.offsetSimultaneousEncoder = offsetSimultaneousEncoder;
        this.version = version;
    }

    protected abstract OffsetEncoding getEncodingType();

    protected abstract OffsetEncoding getEncodingTypeCompressed();

    abstract void encodeIncompleteOffset(final long relativeOffset) throws EncodingNotSupportedException;

    abstract void encodeCompletedOffset(final long relativeOffset) throws EncodingNotSupportedException;

    abstract byte[] serialise() throws EncodingNotSupportedException;

    abstract int getEncodedSize();

    boolean quiteSmall() {
        return this.getEncodedSize() < OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES;
    }

    byte[] compress() throws IOException {
        return OffsetSimpleSerialisation.compressZstd(this.getEncodedBytes());
    }

    void register() throws EncodingNotSupportedException {
        final byte[] bytes = this.serialise();
        final OffsetEncoding encodingType = this.getEncodingType();
        this.register(encodingType, bytes);
    }

    private void register(final OffsetEncoding type, final byte[] bytes) {
        log.debug("Registering {}, with size {}", type, bytes.length);
        EncodedOffsetPair encodedPair = new EncodedOffsetPair(type, ByteBuffer.wrap(bytes));
        offsetSimultaneousEncoder.sortedEncodings.add(encodedPair);
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

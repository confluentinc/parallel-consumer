package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.OffsetEncoding.*;
import static io.confluent.csid.utils.Range.range;

/**
 * Encode with multiple strategies at the same time.
 * <p>
 * Have results in an accessible structure, easily selecting the highest compression.
 */
@Slf4j
class OffsetSimultaneousEncoder {

    /**
     * Size threshold in bytes after which compressing the encodings will be compared
     */
    public static final int LARGE_INPUT_MAP_SIZE_THRESHOLD = 200;

    /**
     * The offsets which have not yet been fully completed and can't have their offset committed
     */
    @Getter
    private final Set<Long> incompleteOffsets;

    /**
     * The lowest committable offset
     */
    private final long lowWaterMark;

    /**
     * The next expected offset to be returned by the broker
     */
    private final long nextExpectedOffset;

    /**
     * Map of different encoding types for the same offset data, used for retrieving the data for the encoding type
     */
    @Getter
    Map<OffsetEncoding, byte[]> encodingMap = new EnumMap<>(OffsetEncoding.class);

    /**
     * Ordered set of the the different encodings, used to quickly retrieve the most compressed encoding
     *
     * @see #packSmallest()
     */
    @Getter
    TreeSet<EncodedOffsetPair> sortedEncodings = new TreeSet<>();

    public OffsetSimultaneousEncoder(long lowWaterMark, Long nextExpectedOffset, Set<Long> incompleteOffsets) {
        this.lowWaterMark = lowWaterMark;
        this.nextExpectedOffset = nextExpectedOffset;
        this.incompleteOffsets = incompleteOffsets;
    }

    /**
     * Highwater mark already encoded in string - {@link OffsetMapCodecManager#makeOffsetMetadataPayload} - so encoding
     * BitSet run length may not be needed, or could be swapped
     * <p/>
     * Simultaneously encodes:
     * <ul>
     * <li>{@link OffsetEncoding#BitSet}</li>
     * <li>{@link OffsetEncoding#RunLength}</li>
     * </ul>
     * Conditionaly encodes compression variants:
     * <ul>
     * <li>{@link OffsetEncoding#BitSetCompressed}</li>
     * <li>{@link OffsetEncoding#RunLengthCompressed}</li>
     * </ul>
     * Currently commented out is {@link OffsetEncoding#ByteArray} as there doesn't seem to be an advantage over
     * BitSet encoding.
     * <p>
     * TODO: optimisation - inline this into the partition iteration loop in {@link WorkManager}
     * <p>
     * TODO: optimisation - could double the run-length range from Short.MAX_VALUE (~33,000) to Short.MAX_VALUE * 2
     *  (~66,000) by using unsigned shorts instead
     */
    @SneakyThrows
    public OffsetSimultaneousEncoder invoke() {
        log.trace("Starting encode of incompletes of {}", this.incompleteOffsets);

        final int length = (int) (this.nextExpectedOffset - this.lowWaterMark);

        if (length > LARGE_INPUT_MAP_SIZE_THRESHOLD) {
            log.debug("~Large input map size: {}", length);
        }

        final Set<Encoder> encoders = new HashSet<>();
        encoders.add(new BitsetEncoder(length));
        encoders.add(new RunLengthEncoder());
        // TODO: Remove? byte buffer seems to always be beaten by BitSet, which makes sense
        // encoders.add(new ByteBufferEncoder(length));

        //
        log.debug("Encode loop start,end: [{},{}] length: {}", this.lowWaterMark, this.nextExpectedOffset, length);
        range(length).forEach(rangeIndex -> {
            final long offset = this.lowWaterMark + rangeIndex;
            if (this.incompleteOffsets.contains(offset)) {
                log.trace("Found an incomplete offset {}", offset);
                encoders.forEach(x -> x.containsIndex(rangeIndex));
            } else {
                encoders.forEach(x -> x.doesNotContainIndex(rangeIndex));
            }
        });

        registerEncodings(encoders);

        // log.trace("Input: {}", inputString);
        log.debug("In order: {}", this.sortedEncodings);

        return this;
    }

    private void registerEncodings(final Set<? extends Encoder> encoders) {
        encoders.forEach(Encoder::register);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        final boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(Encoder::quiteSmall);
        if (noEncodingsAreSmallEnough) {
            encoders.forEach(Encoder::registerCompressed);
        }
    }

    /**
     * Select the smallest encoding, and pack it.
     *
     * @see #packEncoding(EncodedOffsetPair)
     */
    public byte[] packSmallest() {
        final EncodedOffsetPair best = this.sortedEncodings.first();
        log.debug("Compression chosen is: {}", best.encoding.name());
        return packEncoding(best);
    }

    /**
     * Pack the encoded bytes into a magic byte wrapped byte array which indicates the encoding type.
     */
    byte[] packEncoding(final EncodedOffsetPair best) {
        final int magicByteSize = Byte.BYTES;
        final ByteBuffer result = ByteBuffer.allocate(magicByteSize + best.data.capacity());
        result.put(best.encoding.magicByte);
        result.put(best.data);
        return result.array();
    }

    private abstract class Encoder {

        protected abstract OffsetEncoding getEncodingType();

        protected abstract OffsetEncoding getEncodingTypeCompressed();

        abstract void containsIndex(final int rangeIndex);

        abstract void doesNotContainIndex(final int rangeIndex);

        abstract byte[] serialise();

        abstract int getEncodedSize();

        boolean quiteSmall() {
            return this.getEncodedSize() < LARGE_INPUT_MAP_SIZE_THRESHOLD;
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
            OffsetSimultaneousEncoder.this.sortedEncodings.add(new EncodedOffsetPair(type, ByteBuffer.wrap(bytes)));
            OffsetSimultaneousEncoder.this.encodingMap.put(type, bytes);
        }

        @SneakyThrows
        void registerCompressed() {
            final byte[] compressed = compress();
            final OffsetEncoding encodingType = this.getEncodingTypeCompressed();
            this.register(encodingType, compressed);
        }

        protected abstract byte[] getEncodedBytes();
    }

    private class BitsetEncoder extends Encoder {

        private final ByteBuffer wrappedBitsetBytesBuffer;
        private final BitSet bitSet;

        private Optional<byte[]> encodedBytes = Optional.empty();

        public BitsetEncoder(final int length) {
            // prep bit set buffer
            this.wrappedBitsetBytesBuffer = ByteBuffer.allocate(Short.BYTES + ((length / 8) + 1));
            // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
            if (length > Short.MAX_VALUE)
                throw new RuntimeException("Bitset too long to encode: " + length + ". (max: " + Short.MAX_VALUE + ")");
            this.wrappedBitsetBytesBuffer.putShort((short) length);
            bitSet = new BitSet(length);
        }

        @Override
        protected OffsetEncoding getEncodingType() {
            return BitSet;
        }

        @Override
        protected OffsetEncoding getEncodingTypeCompressed() {
            return BitSetCompressed;
        }

        @Override
        public void containsIndex(final int rangeIndex) {
            //noop
        }

        @Override
        public void doesNotContainIndex(final int rangeIndex) {
            bitSet.set(rangeIndex);
        }

        @Override
        public byte[] serialise() {
            final byte[] bitSetArray = this.bitSet.toByteArray();
            this.wrappedBitsetBytesBuffer.put(bitSetArray);
            final byte[] array = this.wrappedBitsetBytesBuffer.array();
            this.encodedBytes = Optional.of(array);
            return array;
        }

        @Override
        public int getEncodedSize() {
            return this.encodedBytes.get().length;
        }

        @Override
        protected byte[] getEncodedBytes() {
            return this.encodedBytes.get();
        }

    }

    private class RunLengthEncoder extends Encoder {

        private final AtomicInteger currentRunLengthCount;
        private final AtomicBoolean previousRunLengthState;
        private final List<Integer> runLengthEncodingIntegers;

        private Optional<byte[]> encodedBytes = Optional.empty();

        public RunLengthEncoder() {
            // run length setup
            currentRunLengthCount = new AtomicInteger();
            previousRunLengthState = new AtomicBoolean(false);
            runLengthEncodingIntegers = new ArrayList<>();
        }

        @Override
        protected OffsetEncoding getEncodingType() {
            return RunLength;
        }

        @Override
        protected OffsetEncoding getEncodingTypeCompressed() {
            return RunLengthCompressed;
        }

        @Override
        public void containsIndex(final int rangeIndex) {
            encodeRunLength(false);
        }

        @Override
        public void doesNotContainIndex(final int rangeIndex) {
            encodeRunLength(true);
        }

        @Override
        public byte[] serialise() {
            runLengthEncodingIntegers.add(currentRunLengthCount.get()); // add tail

            ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(runLengthEncodingIntegers.size() * Short.BYTES);
            for (final Integer i : runLengthEncodingIntegers) {
                final short value = i.shortValue();
                runLengthEncodedByteBuffer.putShort(value);
            }

            byte[] array = runLengthEncodedByteBuffer.array();
            encodedBytes = Optional.of(array);
            return array;
        }

        @Override
        public int getEncodedSize() {
            return encodedBytes.get().length;
        }

        @Override
        protected byte[] getEncodedBytes() {
            return encodedBytes.get();
        }

        private void encodeRunLength(final boolean currentIsComplete) {
            // run length
            boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState.get() == currentIsComplete;
            if (currentOffsetMatchesOurRunLengthState) {
                currentRunLengthCount.getAndIncrement();
            } else {
                previousRunLengthState.set(currentIsComplete);
                runLengthEncodingIntegers.add(currentRunLengthCount.get());
                currentRunLengthCount.set(1); // reset to 1
            }
        }
    }

    private class ByteBufferEncoder extends Encoder {

        private final ByteBuffer bytesBuffer;

        public ByteBufferEncoder(final int length) {
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
        public void containsIndex(final int rangeIndex) {
            this.bytesBuffer.put((byte) 0);
        }

        @Override
        public void doesNotContainIndex(final int rangeIndex) {
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
}

package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;

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
     *  (~66,000) by using unsigned shorts instead (higest representable relative offset is Short.MAX_VALUE because each
     *  runlength entry is a Short)
     */
    @SneakyThrows
    public OffsetSimultaneousEncoder invoke() {
        log.trace("Starting encode of incompletes of {}, base offset is: {}", this.incompleteOffsets, lowWaterMark);

        final int length = (int) (this.nextExpectedOffset - this.lowWaterMark);

        if (length > LARGE_INPUT_MAP_SIZE_THRESHOLD) {
            log.debug("~Large input map size: {}", length);
        }

        final Set<OffsetEncoder> encoders = new HashSet<>();
        encoders.add(new BitsetEncoder(length, this));
        encoders.add(new RunLengthEncoder(this));
        // TODO: Remove? byte buffer seems to always be beaten by BitSet, which makes sense
        // encoders.add(new ByteBufferEncoder(length));

        //
        log.debug("Encode loop offset start,end: [{},{}] length: {}", this.lowWaterMark, this.nextExpectedOffset, length);
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

    private void registerEncodings(final Set<? extends OffsetEncoder> encoders) {
        encoders.forEach(OffsetEncoder::register);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        final boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoder::quiteSmall);
        if (noEncodingsAreSmallEnough) {
            encoders.forEach(OffsetEncoder::registerCompressed);
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

}

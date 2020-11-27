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
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.Version.v1;
import static io.confluent.parallelconsumer.OffsetEncoding.Version.v2;

/**
 * Encode with multiple strategies at the same time.
 * <p>
 * Have results in an accessible structure, easily selecting the highest compression.
 *
 * @see #invoke()
 */
@Slf4j
class OffsetSimultaneousEncoder implements OffsetEncoderContract {

    /**
     * Size threshold in bytes after which compressing the encodings will be compared, as it seems to be typically worth
     * the extra compression step when beyond this size in the source array.
     */
    public static final int LARGE_INPUT_MAP_SIZE_THRESHOLD = 200;

//    /**
//     * The offsets which have not yet been fully completed and can't have their offset committed
//     */
//    @Getter
//    private final Set<Long> incompleteOffsets;

    /**
     * The highest committable offset - the next expected offset to be returned by the broker. So by definition, this
     * index in our offset map we're encoding, is always incomplete.
     */
    @Getter
    private long baseOffset;

    private long highestSucceeded;

    /**
     * The difference between the base offset (the offset to be committed) and the highest seen offset
     */
    private int length;

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
    PriorityQueue<EncodedOffsetData> sortedEncodingData = new PriorityQueue();

    /**
     * Force the encoder to also add the compressed versions. Useful for testing.
     * <p>
     * Visible for testing.
     */
    static boolean compressionForced = false;

    /**
     * The encoders to run
     */
    // TODO do we really need both views?
    final HashSet<OffsetEncoderBase> encoders = new HashSet<>();
    final List<OffsetEncoderBase> sortedEncoders = new ArrayList<>();

    /**
     * @param lowWaterMark The highest committable offset
     */
    public OffsetSimultaneousEncoder(
            long lowWaterMark
            ,
            Long newHighestCompleted

//            ,
    ) {
        if (lowWaterMark < 0)
            lowWaterMark = 0;
        if (newHighestCompleted < 0)
            newHighestCompleted = 0L;
//        this.incompleteOffsets = incompleteOffsets;

        initialise(lowWaterMark, newHighestCompleted);

        addEncoders();
    }

    private int initLength(long currentBaseOffset, long highestCompleted) {
        long longLength = highestCompleted - currentBaseOffset;
        int intLength = (int) longLength;
        // sanity
        if (longLength != intLength)
            throw new IllegalArgumentException("Casting inconsistency");
        return intLength;
    }

    //todo can remove sync?
    private synchronized void initialise(final long currentBaseOffset, long currentHighestCompleted) {
        log.trace("Initialising {},{}", currentBaseOffset, currentHighestCompleted);
        this.baseOffset = currentBaseOffset;
        this.highestSucceeded = currentHighestCompleted;

        this.length = initLength(currentBaseOffset, highestSucceeded);

        if (length > LARGE_INPUT_MAP_SIZE_THRESHOLD) {
            log.debug("~Large input map size: {} (start: {} end: {})", length, this.baseOffset, this.highestSucceeded);
        }
    }

    private void reinitEncoders(final long currentBaseOffset, final long currentHighestCompleted) {
        log.debug("Reinitialise all encoders");
        for (final OffsetEncoderBase encoder : encoders) {
            try {
                encoder.maybeReinitialise(currentBaseOffset, currentHighestCompleted);
            } catch (EncodingNotSupportedException a) {
                log.debug("Cannot use {} encoder with new base {} and highest {}: {}",
                        encoder.getClass().getSimpleName(), currentBaseOffset, currentHighestCompleted, a.getMessage());
                encoder.disable(a);
            }
        }
    }

    private void addEncoders() {
        // still need to register encoders?
//        if (length < 1) {
//            // won't need to encode anything
//            return;
//        } else {
//            log.debug("Adding encoders");
//        }

//        if (!encoders.isEmpty()) {
//            return;
//        }

        try {
            encoders.add(new BitsetEncoder(baseOffset, length, this, v1));
        } catch (BitSetEncodingNotSupportedException a) {
            log.debug("Cannot use {} encoder ({})", BitsetEncoder.class.getSimpleName(), a.getMessage());
        }

        try {
            encoders.add(new BitsetEncoder(baseOffset, length, this, v2));
        } catch (BitSetEncodingNotSupportedException a) {
            log.warn("Cannot use {} encoder ({})", BitsetEncoder.class.getSimpleName(), a.getMessage());
        }

        encoders.add(new RunLengthEncoder(baseOffset, this, v1));
        encoders.add(new RunLengthEncoder(baseOffset, this, v2));

        sortedEncoders.addAll(encoders);
    }

    /**
     * Not enabled as byte buffer seems to always be beaten by BitSet, which makes sense
     * <p>
     * Visible for testing
     */
    void addByteBufferEncoder(long baseOffset) {
        encoders.add(new ByteBufferEncoder(baseOffset, length, this));
    }

//    /**
//     * Highwater mark already encoded in string - {@link OffsetMapCodecManager#makeOffsetMetadataPayload} - so encoding
//     * BitSet run length may not be needed, or could be swapped
//     * <p/>
//     * Simultaneously encodes:
//     * <ul>
//     * <li>{@link OffsetEncoding#BitSet}</li>
//     * <li>{@link OffsetEncoding#RunLength}</li>
//     * </ul>
//     * Conditionaly encodes compression variants:
//     * <ul>
//     * <li>{@link OffsetEncoding#BitSetCompressed}</li>
//     * <li>{@link OffsetEncoding#RunLengthCompressed}</li>
//     * </ul>
//     * Currently commented out is {@link OffsetEncoding#ByteArray} as there doesn't seem to be an advantage over
//     * BitSet encoding.
//     * <p>
//     * TODO: optimisation - inline this into the partition iteration loop in {@link WorkManager}
//     * <p>
//     * TODO: optimisation - could double the run-length range from Short.MAX_VALUE (~33,000) to Short.MAX_VALUE * 2
//     *  (~66,000) by using unsigned shorts instead (higest representable relative offset is Short.MAX_VALUE because each
//     *  runlength entry is a Short)
//     * <p>
//     *  TODO VERY large offests ranges are slow (Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should already be known
//     *
//     * @param currentBaseOffset       to use now, checked for consistency
//     * @param currentHighestCompleted to use now, checked for consistency
//     */
//    public OffsetSimultaneousEncoder runOverIncompletes(Set<Long> incompleteOffsets, final long currentBaseOffset, final long currentHighestCompleted) {
////        checkConditionsHaventChanged(currentBaseOffset, currentHighestCompleted);
//
//        log.debug("Starting encode of incompletes, base offset is: {}, end offset is: {}", baseOffset, highestSucceeded);
//        log.trace("Incompletes are: {}", incompleteOffsets);
//
//        //
//        log.debug("Encode loop offset start,end: [{},{}] length: {}", this.baseOffset, this.highestSucceeded, length);
//        /*
//         * todo refactor this loop into the encoders (or sequential vs non sequential encoders) as RunLength doesn't need
//         *  to look at every offset in the range, only the ones that change from 0 to 1. BitSet however needs to iterate
//         *  the entire range. So when BitSet can't be used, the encoding would be potentially a lot faster as RunLength
//         *  didn't need the whole loop.
//         */
//        range(length).forEach(rangeIndex -> {
//            final long offset = this.baseOffset + rangeIndex;
//            if (incompleteOffsets.contains(offset)) {
//                log.trace("Found an incomplete offset {}", offset);
//                encoders.forEach(x -> {
//                    x.encodeIncompleteOffset(currentBaseOffset, rangeIndex, currentHighestCompleted);
//                });
//            } else {
//                encoders.forEach(x -> {
//                    x.encodeCompleteOffset(currentBaseOffset, rangeIndex, currentHighestCompleted);
//                });
//            }
//        });
//
//        serializeAllEncoders();
//
//        log.debug("In order: {}", this.sortedEncodingData);
//
//        return this;
//    }

    public void serializeAllEncoders() {
        sortedEncodingData.clear();
        List<OffsetEncoderBase> toRemove = new ArrayList<>();
        for (OffsetEncoderBase encoder : encoders) {
//            try {
            encoder.registerSerialisedDataIfEnabled();
//            } catch (EncodingNotSupportedException e) {
//                log.debug("Removing {} encoder, not supported ({})", encoder.getEncodingType().description(), e.getMessage());
//                toRemove.add(encoder);
//            }
        }
        encoders.removeAll(toRemove);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoderBase::quiteSmall);
        if (noEncodingsAreSmallEnough || compressionForced) {
            encoders.forEach(OffsetEncoderBase::registerCompressed);
        }

        log.debug("Encoding results: {}", sortedEncodingData);
    }

    /**
     * Select the smallest encoding, and pack it.
     *
     * @see #packEncoding(EncodedOffsetData)
     */
    public byte[] packSmallest() throws EncodingNotSupportedException {
        if (isNoEncodingNeeded()) {
            // no compression needed, so return empty / zero
            return new byte[]{};
        }
        // todo might be called multiple times, should cache?
        // todo need more granular check on this
        if (sortedEncodingData.isEmpty()) {
            throw new EncodingNotSupportedException("No encodings could be used");
        }
        final EncodedOffsetData best = this.sortedEncodingData.poll();
        log.debug("Compression chosen is: {}", best.encoding.name());
        return packEncoding(best);
    }

    /**
     * Pack the encoded bytes into a magic byte wrapped byte array which indicates the encoding type.
     */
    byte[] packEncoding(final EncodedOffsetData best) {
        final int magicByteSize = Byte.BYTES;
        final ByteBuffer result = ByteBuffer.allocate(magicByteSize + best.data.capacity());
        result.put(best.encoding.magicByte);
        result.put(best.data);
        return result.array();
    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        preCheck(baseOffset, relativeOffset, currentHighestCompleted);
//        if (preEncodeCheckCanSkip(baseOffset, relativeOffset, currentHighestCompleted))
//            return;

        for (final OffsetEncoderBase encoder : encoders) {
            encoder.encodeIncompleteOffset(baseOffset, relativeOffset, currentHighestCompleted);
        }
    }

    @Override
    public void encodeCompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        preCheck(baseOffset, relativeOffset, currentHighestCompleted);
//        if (preEncodeCheckCanSkip(baseOffset, relativeOffset, currentHighestCompleted))
//            return;

        for (final OffsetEncoderBase encoder : encoders) {
//            try {
                encoder.encodeCompleteOffset(baseOffset, relativeOffset, currentHighestCompleted);
//            } catch (EncodingNotSupportedException e) {
//                encoder.disable(e);
//            }
        }
    }

    private void preCheck(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        maybeReinitialise(baseOffset, currentHighestCompleted);
    }

    @Override
    public void maybeReinitialise(final long currentBaseOffset, final long currentHighestCompleted) {
        boolean reinitialise = false;

        long newLength = currentHighestCompleted - currentBaseOffset;
//        if (originalLength != newLength) {
////        if (this.highestSuceeded != currentHighestCompleted) {
//            log.debug("Length of Bitset changed {} to {}",
//                    originalLength, newLength);
//            reinitialise = true;
//        }

        if (this.baseOffset < currentBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to truncate",
                    this.baseOffset, currentBaseOffset);
            reinitialise = true;
        }

        if (currentBaseOffset < this.baseOffset)
            throw new InternalRuntimeError(msg("New base offset {} smaller than previous {}", currentBaseOffset, baseOffset));

        this.highestSucceeded = currentHighestCompleted; // always track, change has no impact on me
        this.length = initLength(currentBaseOffset, highestSucceeded);

        if (reinitialise) {
            initialise(currentBaseOffset, currentHighestCompleted);
        }

        reinitEncoders(currentBaseOffset, currentHighestCompleted);
    }

//    private void mabeyAddEncodersIfMissing() {
//        if (encoders.isEmpty()) {
//            this.addEncodersMaybe();
//        }
//    }

    private boolean preEncodeCheckCanSkip(final long currentBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
//        checkConditionsHaventChanged(currentBaseOffset, currentHighestCompleted);

        return checkIfEncodingNeededBasedOnLowWater(relativeOffset);
    }

//    private void checkConditionsHaventChanged(final long currentBaseOffset, final long currentHighestCompleted) {
//        boolean reinitialise = false;
//
//        if (this.highestSuceeded != currentHighestCompleted) {
//            log.debug("Next expected offset from broker {} has moved to {} - need to reset encoders",
//                    this.highestSuceeded, currentHighestCompleted);
//            reinitialise = true;
//        }
//
//        if (this.baseOffset != currentBaseOffset) {
//            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to reset encoders",
//                    this.baseOffset, currentBaseOffset);
//            reinitialise = true;
//        }
//
//        if (reinitialise) {
//            initialise(currentBaseOffset, currentHighestCompleted);
//        }
//    }

    //todo remove don't think this is ever possible, or throw exception
    private boolean checkIfEncodingNeededBasedOnLowWater(final long relativeOffset) {
        // only encode if this work is above the low water mark
        return relativeOffset <= 0;
    }

    /**
     * @return the packed size of the best encoder, or 0 if no encodings have been performed / needed
     */
    @SneakyThrows
    @Override
    public int getEncodedSize() {
//        if (noEncodingRequiredSoFar) {
//            return 0;
//        } else {
//            OffsetEncoder peek = sortedEncoders.peek();
//            return peek.getEncodedSize();
//        }
        throw new InternalRuntimeError("");
    }

    @Override
    public byte[] getEncodedBytes() {
        return new byte[0];
    }

    @Override
    public int getEncodedSizeEstimate() {
        if (isNoEncodingNeeded() || length < 1) {
            return 0;
        } else {
            if (OffsetMapCodecManager.forcedCodec.isPresent()) {
                OffsetEncoding offsetEncoding = OffsetMapCodecManager.forcedCodec.get();
                // todo - this is rubbish
                OffsetEncoderBase offsetEncoderBase = sortedEncoders.stream().filter(x -> x.getEncodingType().equals(offsetEncoding)).findFirst().get();
                return offsetEncoderBase.getEncodedSizeEstimate();
            } else {
                if (sortedEncoders.isEmpty()) {
                    throw new InternalRuntimeError("No encoders");
                }
                Collections.sort(sortedEncoders);
                OffsetEncoderBase smallestEncoder = sortedEncoders.get(0);
                int smallestSizeEstimate = smallestEncoder.getEncodedSizeEstimate();
                log.debug("Currently estimated smallest codec is {}, needing {} bytes",
                        smallestEncoder.getEncodingType(), smallestSizeEstimate);
                return smallestSizeEstimate;
            }
        }
    }

    private boolean isNoEncodingNeeded() {
        return length < 1;
    }

    public Object getSmallestCodec() {
        Collections.sort(sortedEncoders);
        if (sortedEncoders.isEmpty())
            throw new InternalRuntimeError("No encoders");
        return sortedEncoders.get(0);
    }

    @Override
    public String toString() {
        return msg("{} nextExpected: {}, highest succeeded: {}, encoders:{}", getClass().getSimpleName(), baseOffset, highestSucceeded, encoders);
    }
}

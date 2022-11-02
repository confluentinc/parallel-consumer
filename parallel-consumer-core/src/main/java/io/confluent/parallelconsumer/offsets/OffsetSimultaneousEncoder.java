package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;

/**
 * Encode with multiple strategies at the same time.
 * <p>
 * Have results in an accessible structure, easily selecting the highest compression.
 *
 * @author Antony Stubbs
 * @see #invoke()
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class OffsetSimultaneousEncoder {

    /**
     * Size threshold in bytes after which compressing the encodings will be compared, as it seems to be typically worth
     * the extra compression step when beyond this size in the source array.
     */
    public static final int LARGE_ENCODED_SIZE_THRESHOLD_BYTES = 200;

    /**
     * Size threshold to notice particularly large input maps.
     */
    public static final int LARGE_INPUT_MAP_SIZE = 2_000;

    /**
     * The offsets which have not yet been fully completed and can't have their offset committed - only used to test
     * with {@link Set#contains} (no order requirement, but {@link SortedSet} just in case).
     */
    @Getter
    private final SortedSet<Long> incompleteOffsets;

    /**
     * The lowest committable offset
     */
    @ToString.Include
    private final long lowWaterMark;

    /**
     * The difference between the base offset (the offset to be committed) and the highest seen offset.
     */
    @ToString.Include
    private final long lengthBetweenBaseAndHighOffset;

    /**
     * Map of different encoding types for the same offset data, used for retrieving the data for the encoding type
     */
    @Getter
    Map<OffsetEncoding, byte[]> encodingMap = new EnumMap<>(OffsetEncoding.class);

    /**
     * Ordered set of the different encodings, used to quickly retrieve the most compressed encoding
     *
     * @see #packSmallest()
     */
    @Getter
    SortedSet<EncodedOffsetPair> sortedEncodings = new TreeSet<>();

    /**
     * Force the encoder to also add the compressed versions. Useful for testing.
     * <p>
     * Visible for testing.
     */
    @ToString.Include
    public static boolean compressionForced = false;

    /**
     * Used to prevent tests running in parallel that depends on setting static state in this class. Manipulation of
     * static state in tests needs to be removed to this isn't necessary.
     */
    public static final String COMPRESSION_FORCED_RESOURCE_LOCK = "Value doesn't matter, just needs a constant";

    /**
     * The encoders to run. Concurrent so we can remove encoders while traversing.
     */
    private final ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> activeEncoders;

    public OffsetSimultaneousEncoder(long baseOffsetToCommit, long highestSucceededOffset, SortedSet<Long> incompleteOffsets) {
        this.lowWaterMark = baseOffsetToCommit;
        this.incompleteOffsets = incompleteOffsets;

        //
        if (highestSucceededOffset == KAFKA_OFFSET_ABSENCE) { // nothing succeeded yet
            highestSucceededOffset = baseOffsetToCommit;
        }

        highestSucceededOffset = maybeRaiseOffsetHighestSucceeded(baseOffsetToCommit, highestSucceededOffset);

        lengthBetweenBaseAndHighOffset = highestSucceededOffset - this.lowWaterMark + 1;

        if (lengthBetweenBaseAndHighOffset < 0) {
            // sanity check
            throw new IllegalStateException(msg("Cannot have negative length encoding (calculated length: {}, base offset to commit: {}, highest succeeded offset: {})",
                    lengthBetweenBaseAndHighOffset, baseOffsetToCommit, highestSucceededOffset));
        }

        this.activeEncoders = initEncoders();
    }

    /**
     * Ensure that the {@param #highestSucceededOffset} is always at least a single offset behind the {}@param
     * baseOffsetToCommit}. Needed to allow us to jump over gaps in the partitions such as transaction markers.
     * <p>
     * Under normal operation, it is expected that the highest succeeded offset will generally always be higher than the
     * next expected offset to poll. This is because PC processes records well beyond the
     * {@link PartitionState#getOffsetHighestSequentialSucceeded()} all the time, unless operation in
     * {@link ParallelConsumerOptions.ProcessingOrder#PARTITION} order. So this situation - where the highest succeeded
     * offset is below the next offset to poll at the time of commit - will either be an incredibly rare case: only at
     * the very beginning of processing records, or where ALL records are slow enough or blocked, or in synthetically
     * created scenarios (like test cases).
     */
    private long maybeRaiseOffsetHighestSucceeded(long baseOffsetToCommit, long highestSucceededOffset) {
        long nextExpectedMinusOne = baseOffsetToCommit - 1;

        boolean gapLargerThanOne = highestSucceededOffset < nextExpectedMinusOne;
        if (gapLargerThanOne) {
            long gap = nextExpectedMinusOne - highestSucceededOffset;
            log.debug("Gap detected in partition (highest succeeded: {} while next expected poll offset: {} - gap is {}), probably tx markers. Moving highest succeeded to next expected - 1",
                    highestSucceededOffset,
                    nextExpectedMinusOne,
                    gap);
            // jump straight to the lowest incomplete - 1, allows us to jump over gaps in the partitions such as transaction markers
            highestSucceededOffset = nextExpectedMinusOne;
        }

        return highestSucceededOffset;
    }

    private ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> initEncoders() {
        ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> newEncoders = ConcurrentHashMap.newKeySet();
        if (lengthBetweenBaseAndHighOffset > LARGE_INPUT_MAP_SIZE) {
            log.trace("Relatively large input map size: {} (start: {} end: {})", lengthBetweenBaseAndHighOffset, lowWaterMark, getEndOffsetExclusive());
        }

        addBitsetEncoder(newEncoders, v1);
        addBitsetEncoder(newEncoders, v2);


        newEncoders.add(new RunLengthEncoder(this, v1));
        newEncoders.add(new RunLengthEncoder(this, v2));

        return newEncoders;
    }

    private void addBitsetEncoder(ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> newEncoders, OffsetEncoding.Version version) {
        try {
            newEncoders.add(new BitSetEncoder(lengthBetweenBaseAndHighOffset, this, version));
        } catch (BitSetEncodingNotSupportedException a) {
            log.debug("Cannot construct {} version {} : {}", BitSetEncoder.class.getSimpleName(), version, a.getMessage());
        }
    }

    /**
     * The end offset (exclusive)
     */
    private long getEndOffsetExclusive() {
        return lowWaterMark + lengthBetweenBaseAndHighOffset;
    }

    /**
     * Not enabled as byte buffer seems to always be beaten by BitSet, which makes sense
     * <p>
     * Visible for testing
     */
    void addByteBufferEncoder() {
        try {
            activeEncoders.add(new ByteBufferEncoder(lengthBetweenBaseAndHighOffset, this));
        } catch (ArithmeticException a) {
            log.warn("Cannot use {} encoder ({})", BitSetEncoder.class.getSimpleName(), a.getMessage());
        }
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
     * Conditionally encodes compression variants:
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
     *  (~66,000) by using unsigned shorts instead (highest representable relative offset is Short.MAX_VALUE because each
     *  run-length entry is a Short)
     * <p>
     *  TODO VERY large offset ranges is slow (Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should already be known
     */
    public OffsetSimultaneousEncoder invoke() {
        log.debug("Starting encode of incompletes, base offset is: {}, end offset is: {}", lowWaterMark, getEndOffsetExclusive());
        log.trace("Incompletes are: {}", this.incompleteOffsets);

        //
        log.debug("Encode loop offset start,end: [{},{}] length: {}", this.lowWaterMark, getEndOffsetExclusive(), lengthBetweenBaseAndHighOffset);
        /*
         * todo refactor this loop into the encoders (or sequential vs non sequential encoders) as RunLength doesn't need
         *  to look at every offset in the range, only the ones that change from 0 to 1. BitSet however needs to iterate
         *  the entire range. So when BitSet can't be used, the encoding would be potentially a lot faster as RunLength
         *  didn't need the whole loop.
         */
        Range relativeOffsetsLongRange = range(lengthBetweenBaseAndHighOffset);
        relativeOffsetsLongRange.forEach(relativeOffset -> {
            // range index (relativeOffset) is used as we don't actually encode offsets, we encode the relative offset from the base offset
            final long actualOffset = this.lowWaterMark + relativeOffset;
            final boolean isIncomplete = this.incompleteOffsets.contains(actualOffset);
            activeEncoders.forEach(encoder -> {
                try {
                    if (isIncomplete) {
                        log.trace("Found an incomplete offset {}", actualOffset);
                        encoder.encodeIncompleteOffset(relativeOffset);
                    } else {
                        encoder.encodeCompletedOffset(relativeOffset);
                    }
                } catch (EncodingNotSupportedException e) {
                    log.debug("Error encoding offset {} with encoder {}, removing encoder", actualOffset, encoder, e);
                    activeEncoders.remove(encoder);
                }
            });
        });

        registerEncodings(activeEncoders);

        log.debug("In order: {}", this.sortedEncodings);

        return this;
    }

    private void registerEncodings(final Set<? extends OffsetEncoder> encoders) {
        List<OffsetEncoder> toRemove = new ArrayList<>();
        for (OffsetEncoder encoder : encoders) {
            try {
                encoder.register();
            } catch (EncodingNotSupportedException e) {
                log.debug("Removing {} encoder, not supported ({})", encoder.getEncodingType().description(), e.getMessage());
                toRemove.add(encoder);
            }
        }
        toRemove.forEach(encoders::remove);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoder::quiteSmall);
        if (noEncodingsAreSmallEnough || compressionForced) {
            encoders.forEach(OffsetEncoder::registerCompressed);
        }
    }

    /**
     * Select the smallest encoding, and pack it.
     *
     * @see #packEncoding(EncodedOffsetPair)
     */
    public byte[] packSmallest() throws NoEncodingPossibleException {
        if (sortedEncodings.isEmpty()) {
            throw new NoEncodingPossibleException("No encodings could be used");
        }
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

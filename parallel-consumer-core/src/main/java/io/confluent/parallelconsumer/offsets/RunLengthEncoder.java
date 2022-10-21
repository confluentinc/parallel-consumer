package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;

/**
 * RunLength encoder that leverages the nature of this system.
 * <p>
 * One such nature is that gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because
 * they don't exist, and we'll never see them (they no longer exist in the source partition).
 * <p>
 * Uses {@link Long} for it's underlying structure (in order to support output encoding option using Longs), in order to
 * support the worst case scenarios without running out of run length, as {@link Integer#MAX_VALUE} of 2,147,483,647 (2
 * billion records) is not completely inconceivable, whereas {@link Long#MAX_VALUE} of 9,223,372,036,854,775,807 (~9
 * quintillion / 9 x 10^18) is.
 *
 * @author Antony Stubbs
 */
public class RunLengthEncoder extends OffsetEncoder {

    private long currentRunLengthCount = 0;
    private boolean previousRunLengthState = false;

    @Getter
    private final List<Long> runLengthEncodingLongs;

    private Optional<byte[]> encodedBytes = Optional.empty();

    private final Version version;

    public RunLengthEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(offsetSimultaneousEncoder);
        // run length setup
        runLengthEncodingLongs = new ArrayList<>();
        version = newVersion;
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> RunLength;
            case v2 -> RunLengthV2;
            case v3 -> RunLengthV3;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> RunLengthCompressed;
            case v2 -> RunLengthV2Compressed;
            case v3 -> RunLengthV3Compressed;
        };
    }

    @Override
    public void encodeIncompleteOffset(final long relativeOffset) {
        encodeRunLength(false, relativeOffset);
    }

    @Override
    public void encodeCompletedOffset(final long relativeOffset) {
        encodeRunLength(true, relativeOffset);
    }

    @Override
    public byte[] serialise() throws EncodingNotSupportedException {
        addTail();

        int entryWidth = switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
            case v3 -> Long.BYTES;
        };
        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(runLengthEncodingLongs.size() * entryWidth);

        for (final Long runLength : runLengthEncodingLongs) {
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = runLength.shortValue();
//                    final short shortCastRunlength = MathUtils.toShortExact(runLength);
                    if (runLength != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runLength, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(Math.toIntExact(runLength));
                }
                case v3 -> {
                    runLengthEncodedByteBuffer.putLong(runLength);
                }
            }
        }

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    void addTail() {
        runLengthEncodingLongs.add(currentRunLengthCount);
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    protected byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    long previousRangeIndex = KAFKA_OFFSET_ABSENCE;

    private void encodeRunLength(final boolean currentIsComplete, final long relativeOffset) {
        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
        if (currentOffsetMatchesOurRunLengthState) {
            long delta = relativeOffset - previousRangeIndex;
            currentRunLengthCount += delta;
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingLongs.add(currentRunLengthCount);
            currentRunLengthCount = 1; // reset to 1
        }
        previousRangeIndex = relativeOffset;
    }

    /**
     * Useful for testing
     *
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsets(long originalBaseOffset) {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = false;
        long offsetPosition = originalBaseOffset;
        for (final long run : runLengthEncodingLongs) {
            if (succeeded) {
                final long finalOffsetPosition = offsetPosition;
                LongStream.range(0, run).forEachOrdered(longIndex -> {
                    long newGoodOffset = finalOffsetPosition + longIndex;
                    successfulOffsets.add(newGoodOffset);
                });
            }
            offsetPosition += run;
            succeeded = !succeeded;
        }
        return successfulOffsets;
    }
}

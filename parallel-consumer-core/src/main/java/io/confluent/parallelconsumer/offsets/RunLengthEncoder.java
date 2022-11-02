package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.MathUtils;
import io.confluent.csid.utils.Range;
import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
 * they don't exist, and we'll never see them (they no longer exist in the source partition).
 * <p>
 * Run-length is written "Run-length": https://en.wikipedia.org/wiki/Run-length_encoding
 *
 * @author Antony Stubbs
 */
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class RunLengthEncoder extends OffsetEncoder {

    @ToString.Include
    private long currentRunLengthCount = 0;

    @ToString.Include
    private boolean previousRunLengthState = false;

    @ToString.Include
    @Getter
    private final List<Long> runLengthEncodingLongs;

    private Optional<byte[]> encodedBytes = Optional.empty();

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(offsetSimultaneousEncoder, newVersion);
        // run length setup
        runLengthEncodingLongs = new ArrayList<>();
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
    public void encodeIncompleteOffset(final long relativeOffset) throws EncodingNotSupportedException {
        encodeRunLength(false, relativeOffset);
    }

    @Override
    public void encodeCompletedOffset(final long relativeOffset) throws EncodingNotSupportedException {
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
                    try {
                        runLengthEncodedByteBuffer.putShort(MathUtils.toShortExact(runLength));
                    } catch (ArithmeticException e) {
                        throw new RunLengthV1EncodingNotSupported(msg("Run-length too long for Short ({} vs Short max of {})", runLength, Short.MAX_VALUE));
                    }
                }
                case v2 -> {
                    try {
                        runLengthEncodedByteBuffer.putInt(Math.toIntExact(runLength));
                    } catch (ArithmeticException e) {
                        throw new RunlengthV1EncodingNotSupported(msg("Run-length too long for Integer ({} vs max value of Integer is {})", runLength, Integer.MAX_VALUE), e);
                    }
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
        runLengthEncodingLongs.add(currentRunLengthSize);
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

    private void encodeRunLength(final boolean currentIsComplete, final long relativeOffset) throws EncodingNotSupportedException {
        // run length
        final long delta = relativeOffset - previousRangeIndex;
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
        if (currentOffsetMatchesOurRunLengthState) {
            switch (version) {
                case v1 -> {
                    try {
                        final int deltaAsInt = Math.toIntExact(delta);
                        final int newRunLength = Math.addExact(currentRunLengthSize, deltaAsInt);
                        currentRunLengthSize = MathUtils.toShortExact(newRunLength);
                    } catch (ArithmeticException e) {
                        throw new RunLengthV1EncodingNotSupported(msg("Run-length too big for Short ({} vs max of {})", currentRunLengthSize + delta, Short.MAX_VALUE));
                    }
                }
                case v2 -> {
                    try {
                        currentRunLengthSize = Math.toIntExact(Math.addExact(currentRunLengthSize, delta));
                    } catch (ArithmeticException e) {
                        throw new RunLengthV2EncodingNotSupported(msg("Run-length too big for Integer ({} vs max of {})", currentRunLengthSize, Integer.MAX_VALUE));
                    }
                }
            }
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingLongs.add(currentRunLengthSize);
            currentRunLengthSize = 1; // reset to 1
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
                for (final Long integer : Range.range(run)) {
                    long newGoodOffset = offsetPosition + integer;
                    successfulOffsets.add(newGoodOffset);
                }
            }
            offsetPosition += run;
            succeeded = !succeeded;
        }
        return successfulOffsets;
    }
}

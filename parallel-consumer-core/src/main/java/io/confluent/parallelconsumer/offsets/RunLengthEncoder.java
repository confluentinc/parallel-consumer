package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.MathUtils;
import io.confluent.csid.utils.Range;
import lombok.Getter;

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
 * Run-length is written "Run-length": https://en.wikipedia.org/wiki/Run-length_encoding
 *
 * @author Antony Stubbs
 */
public class RunLengthEncoder extends OffsetEncoder {

    private int currentRunLengthSize = 0;

    private boolean previousRunLengthState = false;

    @Getter
    private final List<Integer> runLengthEncodingIntegers;

    private Optional<byte[]> encodedBytes = Optional.empty();

    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(offsetSimultaneousEncoder);
        // run length setup
        runLengthEncodingIntegers = new ArrayList<>();
        version = newVersion;
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> RunLength;
            case v2 -> RunLengthV2;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> RunLengthCompressed;
            case v2 -> RunLengthV2Compressed;
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
        };
        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(runLengthEncodingIntegers.size() * entryWidth);

        for (final Integer runLength : runLengthEncodingIntegers) {
            switch (version) {
                case v1 -> {
                    try {
                        runLengthEncodedByteBuffer.putShort(MathUtils.toShortExact(runLength));
                    } catch (ArithmeticException e) {
                        throw new RunLengthV1EncodingNotSupported(msg("Run-length too long for Short ({} vs Short max of {})", runLength, Short.MAX_VALUE));
                    }
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(runLength);
                }
            }
        }

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    void addTail() {
        runLengthEncodingIntegers.add(currentRunLengthSize);
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
            runLengthEncodingIntegers.add(currentRunLengthSize);
            currentRunLengthSize = 1; // reset to 1
        }
        previousRangeIndex = relativeOffset;
    }

    /**
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsets(long originalBaseOffset) {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = false;
        long offsetPosition = originalBaseOffset;
        for (final int run : runLengthEncodingIntegers) {
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

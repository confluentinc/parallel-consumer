package io.confluent.parallelconsumer;

import io.confluent.csid.utils.Range;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

/**
 * RunLength encoder that leverages the nature of this system.
 * <p>
 * One such nature is that gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because
 * they don't exist and we'll neve see them (they no longer exist in the source partition).
 */
class RunLengthEncoder extends OffsetEncoder {

    private int currentRunLengthCount = 0;
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
    public void encodeIncompleteOffset(final int rangeIndex) {
        encodeRunLength(false, rangeIndex);
    }

    @Override
    public void encodeCompletedOffset(final int rangeIndex) {
        encodeRunLength(true, rangeIndex);
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
                    final short shortCastRunlength = runLength.shortValue();
                    if (runLength != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runLength, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
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
        runLengthEncodingIntegers.add(currentRunLengthCount);
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    protected byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    int previousRangeIndex = -1;

    private void encodeRunLength(final boolean currentIsComplete, final int rangeIndex) {
        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
        if (currentOffsetMatchesOurRunLengthState) {
            int delta = rangeIndex - previousRangeIndex;
            currentRunLengthCount += delta;
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingIntegers.add(currentRunLengthCount);
            currentRunLengthCount = 1; // reset to 1
        }
        previousRangeIndex = rangeIndex;
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
                for (final Integer integer : Range.range(run)) {
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

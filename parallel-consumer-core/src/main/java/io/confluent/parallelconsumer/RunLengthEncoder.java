package io.confluent.parallelconsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

class RunLengthEncoder extends OffsetEncoder {

    private int currentRunLengthCount = 0;
    private boolean previousRunLengthState = false;

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
        encodeRunLength(false);
    }

    @Override
    public void encodeCompletedOffset(final int rangeIndex) {
        encodeRunLength(true);
    }

    @Override
    public byte[] serialise() throws EncodingNotSupportedException {
        runLengthEncodingIntegers.add(currentRunLengthCount); // add tail

        int entryWidth = switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(runLengthEncodingIntegers.size() * entryWidth);

        for (final Integer runlength : runLengthEncodingIntegers) {
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = runlength.shortValue();
                    if (runlength != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runlength, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(runlength);
                }
            }
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
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
        if (currentOffsetMatchesOurRunLengthState) {
            currentRunLengthCount++;
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingIntegers.add(currentRunLengthCount);
            currentRunLengthCount = 1; // reset to 1
        }
    }
}

package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import static io.confluent.csid.utils.JavaUtils.safeCast;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

/**
 * Encodes a range of offsets, from an incompletes collection into a BitSet.
 * <p>
 * Highly efficient when the completion status is random.
 * <p>
 * Highly inefficient when the completion status is in large blocks ({@link RunLengthEncoder} is much better)
 * <p>
 * Because our system works on manipulating INCOMPLETE offsets, it doesn't matter if the offset range we're encoding is
 * Sequential or not. Because as records are always in commit order, if we've seen a range of offsets, we know we've
 * seen all that exist (within said range). So if offset 8 is missing from the partition, we will encode it as having
 * been completed (when in fact it doesn't exist), because we only compare against known incompletes, and assume all
 * others are complete.
 * <p>
 * So, when we deserialize, the INCOMPLETES collection is then restored, and that's what's used to compare to see if a
 * record should be skipped or not. So if record 8 is recorded as completed, it will be absent from the restored
 * INCOMPLETES list, and we are assured we will never see record 8.
 *
 * @see RunLengthEncoder
 * @see OffsetBitSet
 */
@ToString(onlyExplicitlyIncluded = true, callSuper = true)
@Slf4j
class BitsetEncoder extends OffsetEncoderBase {

    @ToString.Include
    private final Version version; // default to new version

    @ToString.Include
    private static final Version DEFAULT_VERSION = Version.v2;

    public static final Integer MAX_LENGTH_ENCODABLE = Integer.MAX_VALUE;

    @ToString.Include
    private int originalLength;
//    private ByteBuffer wrappedBitsetBytesBuffer;

    BitSet bitSet = new BitSet();

    private Optional<byte[]> encodedBytes = Optional.empty();

    public BitsetEncoder(long baseOffset, int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) throws BitSetEncodingNotSupportedException {
        this(baseOffset, length, offsetSimultaneousEncoder, DEFAULT_VERSION);
    }

    public BitsetEncoder(long baseOffset, int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) throws BitSetEncodingNotSupportedException {
        super(baseOffset, offsetSimultaneousEncoder);

        this.version = newVersion;

        reinitialise(baseOffset, length);
    }

    private ByteBuffer constructWrappedByteBuffer(final int length, final Version newVersion) throws BitSetEncodingNotSupportedException {
        return switch (newVersion) {
            case v1 -> initV1(length);
            case v2 -> initV2(length);
        };
    }

    /**
     * Switch from encoding bitset length as a short to an integer (length of 32,000 was reasonable too short).
     * <p>
     * Integer.MAX_VALUE should always be good enough as system restricts large from being processed at once.
     *
     * @return
     */
    private ByteBuffer initV2(int length) throws BitSetEncodingNotSupportedException {
        if (length > MAX_LENGTH_ENCODABLE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException(msg("Bitset V2 too long to encode, as length overflows Integer.MAX_VALUE. Length: {}. (max: {})", length, MAX_LENGTH_ENCODABLE));
        }

        // prep bit set buffer
        ByteBuffer wrappedBitsetBytesBuffer = ByteBuffer.allocate(Integer.BYTES + ((length / 8) + 1));
        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitsetBytesBuffer.putInt(length);

        return wrappedBitsetBytesBuffer;
    }

    /**
     * This was a bit "short" sighted of me....
     *
     * @return
     */
    private ByteBuffer initV1(int length) throws BitSetEncodingNotSupportedException {
        if (length > Short.MAX_VALUE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException("Bitset V1 too long to encode, bitset length overflows Short.MAX_VALUE: " + length + ". (max: " + Short.MAX_VALUE + ")");
        }

        // prep bit set buffer
        ByteBuffer wrappedBitsetBytesBuffer = ByteBuffer.allocate(Short.BYTES + ((length / 8) + 1));
        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitsetBytesBuffer.putShort((short) length);

        return wrappedBitsetBytesBuffer;
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> BitSet;
            case v2 -> BitSetV2;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> BitSetCompressed;
            case v2 -> BitSetV2Compressed;
        };
    }

    @Override
    public void encodeIncompleteOffset(final long newBaseOffset, final int relativeOffset) {
        // noop - bitset defaults to 0's (`unset`)
    }

    @Override
    public void encodeCompletedOffset(final long newBaseOffset, final int relativeOffset) {
        log.trace("Relative offset set {}", relativeOffset);
        bitSet.set(relativeOffset);
    }

    @SneakyThrows
    @Override
    public byte[] serialise() {
        final byte[] bitSetArray = this.bitSet.toByteArray();
        ByteBuffer wrappedBitsetBytesBuffer = constructWrappedByteBuffer(originalLength, version);
        if (wrappedBitsetBytesBuffer.remaining() < bitSetArray.length)
            throw new InternalRuntimeError("Not enough space in byte array");
        try {
            wrappedBitsetBytesBuffer.put(bitSetArray);
        } catch (BufferOverflowException e) {
            log.error("{}", e);
            throw e;
        }
        final byte[] array = wrappedBitsetBytesBuffer.array();
        this.encodedBytes = Optional.of(array);
        return array;
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//        super(baseOffset, relativeOffset);
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//        super(baseOffset, relativeOffset);
//    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        // noop - bitset defaults to 0's (`unset`)

    }

    @Override
    public void encodeCompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
        try {
            maybeReinitialise(newBaseOffset, currentHighestCompleted);
        } catch (EncodingNotSupportedException e) {
            this.disable(e);
        }

        encodeCompletedOffset(newBaseOffset, safeCast(relativeOffset));
    }

    @Override
    public int getEncodedSize() {
        return this.encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int logicalSize = originalLength / Byte.SIZE;
        return logicalSize + getLengthEntryBytes();
    }

    @Override
    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) throws EncodingNotSupportedException {
        boolean reinitialise = false;

        long newLength = currentHighestCompleted - newBaseOffset;
        if (originalLength != newLength) {
//        if (this.highestSucceeded != currentHighestCompleted) {
            log.debug("Length of Bitset changed {} to {}",
                    originalLength, newLength);
            reinitialise = true;
        }

        if (originalBaseOffset != newBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to shift bitset right",
                    this.originalBaseOffset, newBaseOffset);
            reinitialise = true;
        }

        if (newBaseOffset < originalBaseOffset)
            throw new InternalRuntimeError("");

        if (reinitialise) {
            reinitialise(newBaseOffset, newLength);
        }

    }

    private void reinitialise(final long newBaseOffset, final long newLength) throws BitSetEncodingNotSupportedException {
        if (newLength == -1) {
            log.debug("Nothing to encode, highest successful offset one behind out starting point");
            bitSet = new BitSet();
            this.originalLength = Math.toIntExact(newLength);
            return;
        } else if (newLength < -2) {
            throw new InternalRuntimeError("Invalid state - highest successful too far behind starting point");
        }

        long baseDelta = newBaseOffset - originalBaseOffset;
        // truncate at new relative delta

        int endIndex = safeCast(baseDelta + originalLength + 1);
        int startIndex = (int) baseDelta;
        BitSet truncated = this.bitSet.get(startIndex, endIndex);
        this.bitSet = new BitSet(safeCast(newLength));
        this.bitSet.or(truncated); // fill with old values

//        bitSet = new BitSet(length);

        this.originalLength = Math.toIntExact(newLength);

        // TODO throws away whats returned
        constructWrappedByteBuffer(safeCast(newLength), this.version);

//            this.bitSet = new BitSet((int) newLength);


        enable();
    }

    private int getLengthEntryBytes() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
    }

    @Override
    public byte[] getEncodedBytes() {
        return this.encodedBytes.get();
    }

}

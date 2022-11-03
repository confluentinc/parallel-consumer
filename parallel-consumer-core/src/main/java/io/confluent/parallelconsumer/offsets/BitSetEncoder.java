package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.MathUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import io.confluent.parallelconsumer.state.PartitionState;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;

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
 * others are complete. See {@link PartitionState#incompleteOffsets} for more discussion on this.
 * <p>
 * So, when we deserialize, the INCOMPLETES collection is then restored, and that's what's used to compare to see if a
 * record should be skipped or not. So if record 8 is recorded as completed, it will be absent from the restored
 * INCOMPLETES list, and we are assured we will never see record 8.
 *
 * @author Antony Stubbs
 * @see PartitionState#incompleteOffsets
 * @see RunLengthEncoder
 * @see OffsetBitSet
 */
@ToString(callSuper = true)
@Slf4j
public class BitSetEncoder extends OffsetEncoder {

    private static final Version DEFAULT_VERSION = Version.v2;

    /**
     * {@link BitSet} only supports {@link Integer#MAX_VALUE) bits
     */
    public static final Integer MAX_LENGTH_ENCODABLE = Integer.MAX_VALUE;

    @Getter
    private final BitSet bitSet;

    private final long originalLength;

    private Optional<byte[]> encodedBytes = Optional.empty();

    /**
     * @param length the difference between the highest and lowest offset to be encoded
     */
    public BitSetEncoder(long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) throws BitSetEncodingNotSupportedException {
        super(offsetSimultaneousEncoder, newVersion);

        // prep bit set buffer, range check above
        try {
            bitSet = new BitSet(Math.toIntExact(length));
        } catch (ArithmeticException e) {
            throw new BitSetEncodingNotSupportedException("BitSet only supports " + MAX_LENGTH_ENCODABLE + " bits, but " + length + " were requested", e);
        }

        this.originalLength = length;
    }

    private ByteBuffer constructWrappedByteBuffer(long length, Version newVersion) throws BitSetEncodingNotSupportedException {
        return switch (newVersion) {
            case v1 -> initV1(length);
            case v2 -> initV2(length);
        };
    }

    /**
     * Switch from encoding bitset length as a short to an integer (Short.MAX_VALUE size of 32,000 was too short).
     * <p>
     * Integer.MAX_VALUE is the most we can use, as {@link BitSet} only supports {@link Integer#MAX_VALUE} bits.
     */
    // TODO refactor inivtV2 and V1 together, passing in the Short or Integer
    private ByteBuffer initV2(long bitsetEntriesRequired) throws BitSetEncodingNotSupportedException {
        if (bitsetEntriesRequired > MAX_LENGTH_ENCODABLE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException(StringUtils.msg("BitSet V2 too long to encode, as length overflows Integer.MAX_VALUE. Length: {}. (max: {})", bitsetEntriesRequired, MAX_LENGTH_ENCODABLE));
        }

        int bytesRequiredForEntries = (int) (Math.ceil((double) bitsetEntriesRequired / Byte.SIZE));
        int lengthEntryWidth = Integer.BYTES;
        int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
        final ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.allocate(wrappedBufferLength);

        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitSetBytesBuffer.putInt(Math.toIntExact(bitsetEntriesRequired));

        return wrappedBitSetBytesBuffer;
    }

    /**
     * This was a bit "short" sighted of me.... Encodes the capacity of the bitset as a short, which is only ~32,000
     * bits ({@link Short#MAX_VALUE}).
     */
    private ByteBuffer initV1(long bitsetEntriesRequired) throws BitSetEncodingNotSupportedException {
        if (bitsetEntriesRequired > Short.MAX_VALUE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException("Input too long to encode for BitSet V1, length overflows Short.MAX_VALUE: " + bitsetEntriesRequired + ". (max: " + Short.MAX_VALUE + ")");
        }

        int bytesRequiredForEntries = (int) (Math.ceil((double) bitsetEntriesRequired / Byte.SIZE));
        int lengthEntryWidth = Short.BYTES;
        int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
        final ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.allocate(wrappedBufferLength);

        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitSetBytesBuffer.putShort(MathUtils.toShortExact(bitsetEntriesRequired));

        return wrappedBitSetBytesBuffer;
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
    public void encodeIncompleteOffset(final long relativeOffset) {
        // noop - bitset defaults to 0's (`unset`)
    }

    @Override
    public void encodeCompletedOffset(final long relativeOffset) {
        // range will already have been checked at initialization
        bitSet.set(Math.toIntExact(relativeOffset));
    }

    @Override
    public byte[] serialise() throws BitSetEncodingNotSupportedException {
        final byte[] bitSetArray = this.bitSet.toByteArray();
        ByteBuffer wrappedBitSetBytesBuffer = constructWrappedByteBuffer(originalLength, version);

        if (wrappedBitSetBytesBuffer.remaining() < bitSetArray.length)
            throw new InternalRuntimeException("Not enough space in byte array");

        try {
            wrappedBitSetBytesBuffer.put(bitSetArray);
        } catch (BufferOverflowException e) {
            throw new InternalRuntimeException("Error copying bitset into byte wrapper", e);
        }

        final byte[] array = wrappedBitSetBytesBuffer.array();
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

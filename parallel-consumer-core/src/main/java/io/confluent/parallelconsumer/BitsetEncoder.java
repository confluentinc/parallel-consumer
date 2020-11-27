package io.confluent.parallelconsumer;

import io.confluent.csid.utils.StringUtils;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import static io.confluent.parallelconsumer.OffsetEncoding.BitSetCompressed;

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
class BitsetEncoder extends OffsetEncoder {

    public static final Short MAX_LENGTH_ENCODABLE = Short.MAX_VALUE;

    private final ByteBuffer wrappedBitsetBytesBuffer;
    private final BitSet bitSet;

    private Optional<byte[]> encodedBytes = Optional.empty();

    public BitsetEncoder(int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) throws BitSetEncodingNotSupportedException {
        super(offsetSimultaneousEncoder);
        if (length > MAX_LENGTH_ENCODABLE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException(StringUtils.msg("Bitset too long to encode, as length overflows Short.MAX_VALUE. Length: {}. (max: {})", length, Short.MAX_VALUE));
        }
        // prep bit set buffer
        this.wrappedBitsetBytesBuffer = ByteBuffer.allocate(Short.BYTES + ((length / 8) + 1));
        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        this.wrappedBitsetBytesBuffer.putShort((short) length);
        bitSet = new BitSet(length);
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return OffsetEncoding.BitSet;
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return BitSetCompressed;
    }

    @Override
    public void encodeIncompleteOffset(final int index) {
        // noop - bitset defaults to 0's (`unset`)
    }

    @Override
    public void encodeCompletedOffset(final int index) {
        bitSet.set(index);
    }

    @Override
    public byte[] serialise() {
        final byte[] bitSetArray = this.bitSet.toByteArray();
        this.wrappedBitsetBytesBuffer.put(bitSetArray);
        final byte[] array = this.wrappedBitsetBytesBuffer.array();
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

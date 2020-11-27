package io.confluent.parallelconsumer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import static io.confluent.parallelconsumer.OffsetEncoding.BitSetCompressed;

class BitsetEncoder extends OffsetEncoder {

    private final ByteBuffer wrappedBitsetBytesBuffer;
    private final BitSet bitSet;

    private Optional<byte[]> encodedBytes = Optional.empty();

    public BitsetEncoder(final int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        super(offsetSimultaneousEncoder);
        // prep bit set buffer
        this.wrappedBitsetBytesBuffer = ByteBuffer.allocate(Short.BYTES + ((length / 8) + 1));
        if (length > Short.MAX_VALUE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new RuntimeException("Bitset too long to encode, bitset length overflows Short.MAX_VALUE: " + length + ". (max: " + Short.MAX_VALUE + ")");
        }
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
    public void containsIndex(final int rangeIndex) {
        //noop
    }

    @Override
    public void doesNotContainIndex(final int rangeIndex) {
        bitSet.set(rangeIndex);
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

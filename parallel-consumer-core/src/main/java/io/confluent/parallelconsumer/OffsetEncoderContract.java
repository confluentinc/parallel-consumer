package io.confluent.parallelconsumer;

public interface OffsetEncoderContract {

    /**
     * TODO this method isnt' actually used by any encoder
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted);

    /**
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeCompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted);

    int getEncodedSize();

    byte[] getEncodedBytes();

    /**
     * Used for comparing encoders
     */
    int getEncodedSizeEstimate();

    void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) throws EncodingNotSupportedException;

}

package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.state.PartitionState;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.csid.utils.StringUtils.msg;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Uses multiple encodings to compare, when decided, can refactor other options out for analysis only - {@link
 * #encodeOffsetsCompressed}
 * <p>
 * TODO: consider IO exception management - question sneaky throws usage?
 * <p>
 * TODO: enforce max uncommitted {@literal <} encoding length (Short.MAX)
 * <p>
 * Bitset serialisation format:
 * <ul>
 * <li>byte1: magic
 * <li>byte2-3: Short: bitset size
 * <li>byte4-n: serialised {@link BitSet}
 * </ul>
 */
@Slf4j
public class OffsetMapCodecManager<K, V> {

    /**
     * Used to prevent tests running in parallel that depends on setting static state in this class. Manipulation of
     * static state in tests needs to be removed to this isn't necessary.
     * <p>
     * todo remove static state manipulation from tests (make non static)
     */
    public static final String METADATA_DATA_SIZE_RESOURCE_LOCK = "Value doesn't matter, just needs a constant";

    /**
     * Maximum size of the commit offset metadata
     *
     * @see <a href="https://github.com/apache/kafka/blob/9bc9a37e50e403a356a4f10d6df12e9f808d4fba/core/src/main/scala/kafka/coordinator/group/OffsetConfig.scala#L52">OffsetConfig#DefaultMaxMetadataSize</a>
     * @see "kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize"
     */
    public static int DefaultMaxMetadataSize = 4096;

    public static final Charset CHARSET_TO_USE = UTF_8;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * Decoding result for encoded offsets
     */
    @Value
    public static class HighestOffsetAndIncompletes {

        /**
         * The highest represented offset in this result.
         */
        Long highestSeenOffset;

        /**
         * Of the offsets encoded, the incomplete ones.
         */
        Set<Long> incompleteOffsets;

        public static HighestOffsetAndIncompletes of(Long highestSeenOffset) {
            return new HighestOffsetAndIncompletes(highestSeenOffset, new HashSet<>());
        }

        public static HighestOffsetAndIncompletes of(long highestSeenOffset, Set<Long> incompleteOffsets) {
            return new HighestOffsetAndIncompletes(highestSeenOffset, incompleteOffsets);
        }

        public static HighestOffsetAndIncompletes of() {
            return HighestOffsetAndIncompletes.of(null);
        }
    }

    /**
     * Forces the use of a specific codec, instead of choosing the most efficient one. Useful for testing.
     */
    public static Optional<OffsetEncoding> forcedCodec = Optional.empty();

    public OffsetMapCodecManager(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    /**
     * Load all the previously completed offsets that were not committed
     *
     * @return
     */
    // todo make package private?
    // todo rename
    public Map<TopicPartition, PartitionState<K, V>> loadOffsetMapForPartition(final Set<TopicPartition> assignment) {
        // load last committed state / metadata from consumer
        // todo this should be controlled for - improve consumer management so that this can't happen
        Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets = null;
        int attempts = 0;
        while (lastCommittedOffsets == null) {
            WakeupException lastWakeupException = null;
            try {
                lastCommittedOffsets = consumer.committed(assignment);
            } catch (WakeupException exception) {
                log.warn("Woken up trying to get assignment", exception);
                lastWakeupException = exception;
            }
            attempts++;
            if (attempts > 10) // shouldn't need more than 1 ever
                throw new InternalRuntimeError("Failed to get partition assignment - continuously woken up.", lastWakeupException);
        }

        var states = new HashMap<TopicPartition, PartitionState<K, V>>();
        lastCommittedOffsets.forEach((tp, offsetAndMeta) -> {
            if (offsetAndMeta != null) {
                long nextExpectedOffset = offsetAndMeta.offset();
                String metadata = offsetAndMeta.metadata();
                try {
                    // todo rename
                    PartitionState<K, V> incompletes = decodeIncompletes(nextExpectedOffset, tp, metadata);
                    states.put(tp, incompletes);
                } catch (OffsetDecodingError offsetDecodingError) {
                    log.error("Error decoding offsets from assigned partition, dropping offset map (will replay previously completed messages - partition: {}, data: {})",
                            tp, offsetAndMeta, offsetDecodingError);
                }
            }

        });

        // for each assignment which isn't now added in the states to return, enter a default entry. Catches multiple other cases.
        assignment.stream()
                .filter(x -> !states.containsKey(x))
                .forEach(x ->
                        states.put(x, new PartitionState<>(x, HighestOffsetAndIncompletes.of())));

        return states;
    }

    static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition, String base64EncodedOffsetPayload) throws OffsetDecodingError {
        byte[] decodedBytes;
        try {
            decodedBytes = OffsetSimpleSerialisation.decodeBase64(base64EncodedOffsetPayload);
        } catch (IllegalArgumentException a) {
            throw new OffsetDecodingError(msg("Error decoding offset metadata, input was: {}", base64EncodedOffsetPayload), a);
        }
        return decodeCompressedOffsets(committedOffsetForPartition, decodedBytes);
    }

    // todo rename
    PartitionState<K, V> decodeIncompletes(long nextExpectedOffset, TopicPartition tp, String offsetMetadataPayload) throws OffsetDecodingError {
        HighestOffsetAndIncompletes incompletes = deserialiseIncompleteOffsetMapFromBase64(nextExpectedOffset, offsetMetadataPayload);
        log.debug("Loaded incomplete offsets from offset payload {}", incompletes);
        return new PartitionState<K, V>(tp, incompletes);
    }

    public String makeOffsetMetadataPayload(long finalOffsetForPartition, PartitionState<K, V> state) throws EncodingNotSupportedException {
        String offsetMap = serialiseIncompleteOffsetMapToBase64(finalOffsetForPartition, state);
        return offsetMap;
    }

    String serialiseIncompleteOffsetMapToBase64(long finalOffsetForPartition, PartitionState<K, V> state) throws EncodingNotSupportedException {
        byte[] compressedEncoding = encodeOffsetsCompressed(finalOffsetForPartition, state);
        String b64 = OffsetSimpleSerialisation.base64(compressedEncoding);
        return b64;
    }

    /**
     * Print out all the offset status into a String, and use X to effectively do run length encoding compression on the
     * string.
     * <p>
     * Include the magic byte in the returned array.
     * <p>
     * Can remove string encoding in favour of the boolean array for the `BitSet` if that's how things settle.
     */
    byte[] encodeOffsetsCompressed(long finalOffsetForPartition, PartitionState<K, V> partition) throws EncodingNotSupportedException {
        TopicPartition tp = partition.getTp();
        Set<Long> incompleteOffsets = partition.getIncompleteOffsets();
        log.debug("Encoding partition {} incomplete offsets {}", tp, incompleteOffsets);
        long offsetHighestSucceeded = partition.getOffsetHighestSucceeded();
        OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, offsetHighestSucceeded, incompleteOffsets).invoke();

        //
        if (forcedCodec.isPresent()) {
            var forcedOffsetEncoding = forcedCodec.get();
            log.debug("Forcing use of {}, for testing", forcedOffsetEncoding);
            Map<OffsetEncoding, byte[]> encodingMap = simultaneousEncoder.getEncodingMap();
            byte[] bytes = encodingMap.get(forcedOffsetEncoding);
            if (bytes == null)
                throw new EncodingNotSupportedException(msg("Can't force an encoding that hasn't been run: {}", forcedOffsetEncoding));
            return simultaneousEncoder.packEncoding(new EncodedOffsetPair(forcedOffsetEncoding, ByteBuffer.wrap(bytes)));
        } else {
            return simultaneousEncoder.packSmallest();
        }
    }

    /**
     * Print out all the offset status into a String, and potentially use zstd to effectively do run length encoding
     * compression
     *
     * @return Set of offsets which are not complete, and the highest offset encoded.
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset, byte[] decodedBytes) {

        // if no offset bitmap data
        if (decodedBytes.length == 0) {
            // in this case, as there is no encoded offset data in the matadata, the highest we previously saw must be
            // the offset before the committed offset
            long highestSeenOffsetIsThen = nextExpectedOffset - 1;
            return HighestOffsetAndIncompletes.of(highestSeenOffsetIsThen);
        } else {
            var result = EncodedOffsetPair.unwrap(decodedBytes);

            HighestOffsetAndIncompletes incompletesTuple = result.getDecodedIncompletes(nextExpectedOffset);

            Set<Long> incompletes = incompletesTuple.getIncompleteOffsets();
            long highWater = incompletesTuple.getHighestSeenOffset();

            return HighestOffsetAndIncompletes.of(highWater, incompletes);
        }
    }

    String incompletesToBitmapString(long finalOffsetForPartition, PartitionState<K, V> state) {
        var runLengthString = new StringBuilder();
        Long lowWaterMark = finalOffsetForPartition;
        Long highWaterMark = state.getOffsetHighestSeen();
        long end = highWaterMark - lowWaterMark;
        for (final var relativeOffset : range(end)) {
            long offset = lowWaterMark + relativeOffset;
            if (state.getIncompleteOffsets().contains(offset)) {
                runLengthString.append("o");
            } else {
                runLengthString.append("x");
            }
        }
        return runLengthString.toString();
    }

    static Set<Long> bitmapStringToIncomplete(final long baseOffset, final String inputBitmapString) {
        final Set<Long> incompleteOffsets = new HashSet<>();

        final long longLength = inputBitmapString.length();
        range(longLength).forEach(i -> {
            var bit = inputBitmapString.charAt(i);
            if (bit == 'o') {
                incompleteOffsets.add(baseOffset + i);
            } else if (bit == 'x') {
                log.trace("Dropping completed offset");
            } else {
                throw new IllegalArgumentException("Invalid encoding - unexpected char: " + bit);
            }
        });

        return incompleteOffsets;
    }

}

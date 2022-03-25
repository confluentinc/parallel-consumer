package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
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
import java.util.stream.Collectors;

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

    // todo OffsetMapCodecManager needs refactoring - consumer presence here smells bad #233
    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * Decoding result for encoded offsets
     */
    @Value
    public static class HighestOffsetAndIncompletes {

        /**
         * The highest represented offset in this result.
         */
        Optional<Long> highestSeenOffset;

        /**
         * Of the offsets encoded, the incomplete ones.
         */
        Set<Long> incompleteOffsets;

        public static HighestOffsetAndIncompletes of(long highestSeenOffset) {
            return new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), new HashSet<>());
        }

        public static HighestOffsetAndIncompletes of(long highestSeenOffset, Set<Long> incompleteOffsets) {
            return new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), incompleteOffsets);
        }

        public static HighestOffsetAndIncompletes of() {
            return new HighestOffsetAndIncompletes(Optional.empty(), new HashSet<>());
        }
    }

    /**
     * Forces the use of a specific codec, instead of choosing the most efficient one. Useful for testing.
     */
    public static Optional<OffsetEncoding> forcedCodec = Optional.empty();

    // todo remove consumer #233
    public OffsetMapCodecManager(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    /**
     * Load all the previously completed offsets that were not committed
     */
    // todo this is the only method that needs the consumer - offset encoding is being conflated with decoding upon assignment #233
    // todo make package private?
    // todo rename
    public Map<TopicPartition, PartitionState<K, V>> loadPartitionStateForAssignment(final Collection<TopicPartition> assignment) {
        // load last committed state / metadata from consumer
        // todo this should be controlled for - improve consumer management so that this can't happen
        Map<TopicPartition, OffsetAndMetadata> partitionLastCommittedOffsets = null;
        int attempts = 0;
        while (partitionLastCommittedOffsets == null) {
            WakeupException lastWakeupException = null;
            try {
                partitionLastCommittedOffsets = consumer.committed(new HashSet<>(assignment));
            } catch (WakeupException exception) {
                log.debug("Woken up trying to get assignment", exception);
                lastWakeupException = exception;
            }
            attempts++;
            if (attempts > 10) // shouldn't need more than 1 ever
                throw new InternalRuntimeError("Failed to get partition assignment - continuously woken up.", lastWakeupException);
        }

        var partitionStates = new HashMap<TopicPartition, PartitionState<K, V>>();
        partitionLastCommittedOffsets.forEach((tp, offsetAndMeta) -> {
            if (offsetAndMeta != null) {
                try {
                    PartitionState<K, V> state = decodePartitionState(tp, offsetAndMeta);
                    partitionStates.put(tp, state);
                } catch (OffsetDecodingError offsetDecodingError) {
                    log.error("Error decoding offsets from assigned partition, dropping offset map (will replay previously completed messages - partition: {}, data: {})",
                            tp, offsetAndMeta, offsetDecodingError);
                }
            }

        });

        // assigned partitions for which there has never been a commit
        // for each assignment with no commit history, enter a default entry. Catches multiple other cases.
        assignment.stream()
                .filter(topicPartition -> !partitionStates.containsKey(topicPartition))
                .forEach(topicPartition -> {
                    PartitionState<K, V> defaultEntry = new PartitionState<>(topicPartition, HighestOffsetAndIncompletes.of());
                    partitionStates.put(topicPartition, defaultEntry);
                });

        return partitionStates;
    }

    private HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(OffsetAndMetadata offsetData) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(offsetData.offset(), offsetData.metadata());
    }

    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition, String base64EncodedOffsetPayload) throws OffsetDecodingError {
        byte[] decodedBytes;
        try {
            decodedBytes = OffsetSimpleSerialisation.decodeBase64(base64EncodedOffsetPayload);
        } catch (IllegalArgumentException a) {
            throw new OffsetDecodingError(msg("Error decoding offset metadata, input was: {}", base64EncodedOffsetPayload), a);
        }
        return decodeCompressedOffsets(committedOffsetForPartition, decodedBytes);
    }

    PartitionState<K, V> decodePartitionState(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        HighestOffsetAndIncompletes incompletes = deserialiseIncompleteOffsetMapFromBase64(offsetData);
        log.debug("Loaded incomplete offsets from offset payload {}", incompletes);
        return new PartitionState<K, V>(tp, incompletes);
    }

    public String makeOffsetMetadataPayload(long finalOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        String offsetMap = serialiseIncompleteOffsetMapToBase64(finalOffsetForPartition, state);
        return offsetMap;
    }

    String serialiseIncompleteOffsetMapToBase64(long finalOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
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
    byte[] encodeOffsetsCompressed(long finalOffsetForPartition, PartitionState<K, V> partitionState) throws NoEncodingPossibleException {
        var incompleteOffsets = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
        long highestSucceeded = partitionState.getOffsetHighestSucceeded();
        if (log.isDebugEnabled()) {
            log.debug("Encoding partition {}, highest succeeded {}, incomplete offsets to encode {}",
                    partitionState.getTp(),
                    highestSucceeded,
                    incompleteOffsets.stream().filter(x -> x < highestSucceeded).collect(Collectors.toList()));
        }
        OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highestSucceeded, incompleteOffsets).invoke();

        //
        if (forcedCodec.isPresent()) {
            var forcedOffsetEncoding = forcedCodec.get();
            log.debug("Forcing use of {}, for testing", forcedOffsetEncoding);
            Map<OffsetEncoding, byte[]> encodingMap = simultaneousEncoder.getEncodingMap();
            byte[] bytes = encodingMap.get(forcedOffsetEncoding);
            if (bytes == null)
                throw new NoEncodingPossibleException(msg("Can't force an encoding that hasn't been run: {}", forcedOffsetEncoding));
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
            return result.getDecodedIncompletes(nextExpectedOffset);
        }
    }

}

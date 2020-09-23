package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.ParallelConsumer.Tuple;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.charset.Charset;
import java.util.*;

import static io.confluent.csid.utils.Range.range;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Uses multiple encodings to compare, when decided, can refactor other options out for analysis only - {@link
 * #encodeOffsetsCompressed}
 * <p>
 * TODO: consider IO exception management - question sneaky throws usage?
 * <p>
 * TODO: enforce max uncommitted < encoding length (Short.MAX)
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
     * Maximum size of the commit offset metadata
     *
     * @link https://github.com/apache/kafka/blob/9bc9a37e50e403a356a4f10d6df12e9f808d4fba/core/src/main/scala/kafka/coordinator/group/OffsetConfig.scala#L52
     * @link kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize
     */
    public static final int DefaultMaxMetadataSize = 4096;

    public static final Charset CHARSET_TO_USE = UTF_8;
    private final WorkManager<K, V> wm;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    public OffsetMapCodecManager(final WorkManager<K, V> wm, final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.wm = wm;
        this.consumer = consumer;
    }

    /**
     * Load all the previously completed offsets that were not committed
     */
    void loadAllAssignedOffsetMap() {
        Set<TopicPartition> assignment = consumer.assignment();
        loadOffsetMapForPartition(assignment);
    }

    /**
     * Load all the previously completed offsets that were not committed
     */
    void loadOffsetMapForPartition(final Set<TopicPartition> assignment) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        committed.forEach((tp, offsetAndMeta) -> {
            if (offsetAndMeta != null) {
                long offset = offsetAndMeta.offset();
                String metadata = offsetAndMeta.metadata();
                loadOffsetMetadataPayload(offset, tp, metadata);
            }
        });
    }

    static Tuple<Long, TreeSet<Long>> deserialiseIncompleteOffsetMapFromBase64(long finalBaseComittedOffsetForPartition, String incompleteOffsetMap) {
        byte[] decode = Base64.getDecoder().decode(incompleteOffsetMap);
        Tuple<Long, Set<Long>> incompleteOffsets = decodeCompressedOffsets(finalBaseComittedOffsetForPartition, decode);
        TreeSet<Long> longs = new TreeSet<>(incompleteOffsets.getRight());
        return Tuple.pairOf(incompleteOffsets.getLeft(), longs);
    }

    void loadOffsetMetadataPayload(long startOffset, TopicPartition tp, String offsetMetadataPayload) {
        Tuple<Long, TreeSet<Long>> incompletes = deserialiseIncompleteOffsetMapFromBase64(startOffset, offsetMetadataPayload);
        wm.raisePartitionHighWaterMark(incompletes.getLeft(), tp);
        wm.partitionIncompleteOffsets.put(tp, incompletes.getRight());
    }

    String makeOffsetMetadataPayload(long finalOffsetForPartition, TopicPartition tp, Set<Long> incompleteOffsets) {
        String offsetMap = serialiseIncompleteOffsetMapToBase64(finalOffsetForPartition, tp, incompleteOffsets);
        return offsetMap;
    }

    @SneakyThrows
    String serialiseIncompleteOffsetMapToBase64(long finalOffsetForPartition, TopicPartition tp, Set<Long> incompleteOffsets) {
        byte[] compressedEncoding = encodeOffsetsCompressed(finalOffsetForPartition, tp, incompleteOffsets);
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
    @SneakyThrows
    byte[] encodeOffsetsCompressed(long finalOffsetForPartition, TopicPartition tp, Set<Long> incompleteOffsets) {
        Long nextExpectedOffset = wm.partitionOffsetHighWaterMarks.get(tp) + 1;
        OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, nextExpectedOffset, incompleteOffsets).invoke();
        byte[] result = simultaneousEncoder.packSmallest();

        return result;
    }

    /**
     * Print out all the offset status into a String, and potentially use zstd to effectively do run length encoding
     * compression
     *
     * @return Set of offsets which are not complete.
     */
    static Tuple<Long, Set<Long>> decodeCompressedOffsets(long finalOffsetForPartition, byte[] s) {
        if (s.length == 0) {
            // no offset bitmap data
            return Tuple.pairOf(finalOffsetForPartition, UniSets.of());
        }

        EncodedOffsetPair result = EncodedOffsetPair.unwrap(s);

        Tuple<Long, Set<Long>> incompletesTuple = result.getDecodedIncompletes(finalOffsetForPartition);

        Set<Long> incompletes = incompletesTuple.getRight();
        long highWater = incompletesTuple.getLeft();

        Tuple<Long, Set<Long>> tuple = Tuple.pairOf(highWater, incompletes);
        return tuple;
    }

    String incompletesToBitmapString(long finalOffsetForPartition, TopicPartition tp, Set<Long> incompleteOffsets) {
        StringBuilder runLengthString = new StringBuilder();
        Long lowWaterMark = finalOffsetForPartition;
        Long highWaterMark = wm.partitionOffsetHighWaterMarks.get(tp);
        long end = highWaterMark - lowWaterMark;
        for (final var relativeOffset : range(end)) {
            long offset = lowWaterMark + relativeOffset;
            if (incompleteOffsets.contains(offset)) {
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
            char bit = inputBitmapString.charAt(i);
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

package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.JavaUtils;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class OffsetEncodingTests extends ParallelEoSStreamProcessorTestBase {

    @Test
    void runLengthDeserialise() {
        var sb = ByteBuffer.allocate(3);
        sb.put((byte) 0); // magic byte placeholder, can ignore
        sb.putShort((short) 1);
        byte[] array = new byte[2];
        sb.rewind();
        sb.get(array);
        ByteBuffer wrap = ByteBuffer.wrap(array);
        byte b = wrap.get(); // simulate reading magic byte
        ByteBuffer slice = wrap.slice();
        List<Integer> integers = OffsetRunLength.runLengthDeserialise(slice);
        assertThat(integers).isEmpty();
    }

    /**
     * Triggers Short shortfall in BitSet encoder and tests encodable range of RunLength encoding - system should
     * gracefully drop runlength if it has Short overflows (too hard to measure every runlength of incoming records
     * before accepting?)
     * <p>
     * https://github.com/confluentinc/parallel-consumer/issues/37 Support BitSet encoding lengths longer than
     * Short.MAX_VALUE #37
     * <p>
     * https://github.com/confluentinc/parallel-consumer/issues/35 RuntimeException when running with very high options
     * in 0.2.0.0 (Bitset too long to encode) #35
     * <p>
     */
    @SneakyThrows
    @ParameterizedTest
    @ValueSource(longs = {
            10_000L,
            100_000L,
            100_000_0L,
//            100_000_000L, // very~ slow
    })
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void largeIncompleteOffsetValues(long nextExpectedOffset) {
        var incompletes = new HashSet<Long>();
        long lowWaterMark = 123L;
        incompletes.addAll(UniSets.of(lowWaterMark, 2345L, 8765L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        OffsetSimultaneousEncoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                OffsetMapCodecManager.HighestOffsetAndIncompletes decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getIncompleteOffsets())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * There's no guarantee that offsets are always sequential. The most obvious case is with a compacted topic - there
     * will always be offsets missing.
     *
     * @see #ensureEncodingGracefullyWorksWhenOffsetsArentSequentialTwo
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource(OffsetEncoding.class)
    // needed due to static accessors in parallel tests
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ)
    // depends on OffsetMapCodecManager#DefaultMaxMetadataSize
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential(OffsetEncoding encoding) {
        assumeThat("Codec skipped, not applicable", encoding,
                not(in(of(ByteArray, ByteArrayCompressed)))); // byte array not currently used

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);
        OffsetSimultaneousEncoder.compressionForced = true;

        var records = new ArrayList<ConsumerRecord<String, String>>();
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 0, "akey", "avalue")); // will complete
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 1, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 4, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 5, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 69, "akey", "avalue")); // will complete
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 100, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 1_000, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 20_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 25_000, "akey", "avalue")); // will complete, near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 30_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE

        // Extremely large tests for v2 encoders
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 40_000, "akey", "avalue")); // higher than Short.MAX_VALUE
        int avoidOffByOne = 2;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 40_000 + Short.MAX_VALUE + avoidOffByOne, "akey", "avalue")); // runlength higher than Short.MAX_VALUE
        int highest = 40_000 + Short.MAX_VALUE + avoidOffByOne + 1;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, highest, "akey", "avalue")); // will complete to force whole encoding


        var incompleteRecords = new ArrayList<>(records);
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 0).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 69).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 25_000).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == highest).findFirst().get());

        List<Long> expected = incompleteRecords.stream().map(ConsumerRecord::offset)
                .sorted()
                .collect(Collectors.toList());

        //
        ktu.send(consumerSpy, records);

        //
        ParallelConsumerOptions<String, String> options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, records);
        ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(recordsMap);

        // write offsets
        {
            WorkManager<String, String> wmm = new WorkManager<>(options, consumerSpy);
            wmm.onPartitionsAssigned(UniSets.of(new TopicPartition(INPUT_TOPIC, 0)));
            wmm.registerWork(testRecords);

            List<WorkContainer<String, String>> work = wmm.getWorkIfAvailable();
            assertThat(work).hasSameSizeAs(records);

            KafkaTestUtils.completeWork(wmm, work, 0);

            KafkaTestUtils.completeWork(wmm, work, 69);

            KafkaTestUtils.completeWork(wmm, work, 25_000);

            KafkaTestUtils.completeWork(wmm, work, highest);


            // make the commit
            var completedEligibleOffsetsAndRemove = wmm.findCompletedEligibleOffsetsAndRemove();
            var remap = JavaUtils.remap(completedEligibleOffsetsAndRemove, PartitionState.OffsetPair::getSync);
            assertThat(remap.get(tp).offset()).isEqualTo(1L);
            consumerSpy.commitSync(remap);

            {
                // check for graceful fall back to the smallest available encoder
                OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(consumerSpy);
                OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
                var state = wmm.getPm().getPartitionState(tp);
                String bestPayload = om.makeOffsetMetadataPayload(1, state);
                assertThat(bestPayload).isNotEmpty();
            }
        }

        // check
        {
            var committed = consumerSpy.committed(UniSets.of(tp)).get(tp);
            assertThat(committed.offset()).isEqualTo(1L);
            // todo why not blank - only some are - check when done
//            assertThat(committed.metadata()).isNotBlank();
        }

        // simulate a rebalance or some sort of reset, by instantiating a new WM with the state from the last

        // read offsets
        {
            var newWm = new WorkManager<>(options, consumerSpy);
            newWm.onPartitionsAssigned(UniSets.of(tp));
            newWm.registerWork(testRecords);

            var pm = newWm.getPm();
            var partitionState = pm.getPartitionState(tp);

            var encodingsThatFail = UniLists.of(BitSet, BitSetCompressed, BitSetV2, RunLength, RunLengthCompressed);
            if (!encodingsThatFail.contains(encoding)) {
                long offsetHighestSequentialSucceeded = partitionState.getOffsetHighestSequentialSucceeded();
                assertThat(offsetHighestSequentialSucceeded).isEqualTo(0);

                long offsetHighestSucceeded = partitionState.getOffsetHighestSucceeded();
                assertThat(offsetHighestSucceeded).isEqualTo(highest);

                long offsetHighestSeen = partitionState.getOffsetHighestSeen();
                assertThat(offsetHighestSeen).isEqualTo(highest);

                var incompletes = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
                Truth.assertThat(incompletes).containsExactlyElementsIn(expected);
            }

            // check record is marked as incomplete
            var anIncompleteRecord = records.get(3);
            Truth.assertThat(pm.isRecordPreviouslyCompleted(anIncompleteRecord)).isFalse();

            // force ingestion early, and check results
            {
                int ingested = newWm.tryToEnsureQuantityOfWorkQueuedAvailable(Integer.MAX_VALUE);

                if (!encodingsThatFail.contains(encoding)) {
                    long offsetHighestSequentialSucceeded = partitionState.getOffsetHighestSequentialSucceeded();
                    assertThat(offsetHighestSequentialSucceeded).isEqualTo(0);

                    long offsetHighestSucceeded = partitionState.getOffsetHighestSucceeded();
                    assertThat(offsetHighestSucceeded).isEqualTo(highest);

                    long offsetHighestSeen = partitionState.getOffsetHighestSeen();
                    assertThat(offsetHighestSeen).isEqualTo(highest);

                    var incompletes = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
                    Truth.assertThat(incompletes).containsExactlyElementsIn(expected);

                    assertThat(ingested).isEqualTo(testRecords.count() - 4); // 4 were succeeded
                    Truth.assertThat(pm.isRecordPreviouslyCompleted(anIncompleteRecord)).isFalse();
                }
            }


            var workRetrieved = newWm.getWorkIfAvailable();
            var workRetrievedOffsets = workRetrieved.stream().map(WorkContainer::offset).collect(Collectors.toList());
            Truth.assertThat(workRetrieved).isNotEmpty();

            switch (encoding) {
                case BitSet, BitSetCompressed, // BitSetV1 both get a short overflow due to the length being too long
                        BitSetV2, // BitSetv2 uncompressed is too large to fit in metadata payload
                        RunLength, RunLengthCompressed // RunLength V1 max runlength is Short.MAX_VALUE
                        -> {
                    assertThat(workRetrievedOffsets).doesNotContain(2500L);
                    assertThat(workRetrievedOffsets).doesNotContainSequence(expected);

                    // delete
                    assertThatThrownBy(() -> {
                        assertThat(workRetrievedOffsets)
                                .containsExactlyElementsOf(expected);

//                        assertThat(workRetrieved).extracting(WorkContainer::getCr)
//                                .containsExactlyElementsOf(incompleteRecords);
                    })
                            .hasMessageContaining("but some elements were not")
                            .hasMessageContaining("25000L");

                }
                default -> {
                    assertThat(workRetrievedOffsets)
                            .as("Contains only incomplete records")
                            .containsExactlyElementsOf(expected);
                }
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * This version of non sequential test just test the encoder directly, and is only half the story, as at the
     * encoding stage they don't know which offsets have never been seen, and assume simply working with continuous
     * ranges.
     * <p>
     * See more info in the class javadoc of {@link BitsetEncoder}.
     *
     * @see BitsetEncoder
     * @see #ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential
     */
    @SneakyThrows
    @Test
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void ensureEncodingGracefullyWorksWhenOffsetsArentSequentialTwo() {
        long nextExpectedOffset = 101;
        long lowWaterMark = 0;
        var incompletes = new HashSet<>(UniSets.of(1L, 4L, 5L, 100L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        OffsetSimultaneousEncoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);

        if (nextExpectedOffset - lowWaterMark > BitSetEncoder.MAX_LENGTH_ENCODABLE)
            assertThat(encodingMap.keySet()).as("Gracefully ignores that BitSet can't be supported").doesNotContain(OffsetEncoding.BitSet);
        else
            assertThat(encodingMap.keySet()).contains(OffsetEncoding.BitSet);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                OffsetMapCodecManager.HighestOffsetAndIncompletes decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getIncompleteOffsets())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

}

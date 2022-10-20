package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
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
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @author Antony Stubbs
 */
@Slf4j
public class OffsetEncodingTests extends ParallelEoSStreamProcessorTestBase {

    PCModuleTestEnv module = new PCModuleTestEnv();

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
        long lowWaterMark = 123L;
        var incompletes = new TreeSet<>(UniSets.of(lowWaterMark, 2345L, 8765L));

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
     * Test for offset encoding when there is a very large range of offsets, and where the offsets aren't sequential.
     * <p>
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
        var encodingsThatFail = UniLists.of(BitSet, BitSetCompressed, BitSetV2, RunLength, RunLengthCompressed);

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);
        OffsetSimultaneousEncoder.compressionForced = true;

        var records = new ArrayList<ConsumerRecord<String, String>>();
        final int FIRST_SUCCEEDED_OFFSET = 0;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, FIRST_SUCCEEDED_OFFSET, "akey", "avalue")); // will complete
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
        int highestSucceeded = 40_000 + Short.MAX_VALUE + avoidOffByOne + 1;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, highestSucceeded, "akey", "avalue")); // will complete to force whole encoding


        var incompleteRecords = new ArrayList<>(records);
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == FIRST_SUCCEEDED_OFFSET).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 69).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 25_000).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == highestSucceeded).findFirst().get());

        List<Long> expected = incompleteRecords.stream().map(ConsumerRecord::offset)
                .sorted()
                .collect(Collectors.toList());

        //
        ktu.send(consumerSpy, records);

        //
        ParallelConsumerOptions<String, String> options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, new ArrayList<>(records));
        ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(recordsMap);

        // write offsets
        final ParallelConsumerOptions<String, String> newOptions = options.toBuilder().consumer(consumerSpy).build();
        final long FIRST_COMMITTED_OFFSET = 1L;
        {
            final PCModule<String, String> moduleTwo = new PCModule<>(newOptions);
            WorkManager<String, String> wmm = moduleTwo.workManager();
            wmm.onPartitionsAssigned(UniSets.of(new TopicPartition(INPUT_TOPIC, 0)));
            wmm.registerWork(new EpochAndRecordsMap<>(testRecords, wmm.getPm()));

            List<WorkContainer<String, String>> work = wmm.getWorkIfAvailable();
            assertThat(work).hasSameSizeAs(records);

            KafkaTestUtils.completeWork(wmm, work, FIRST_SUCCEEDED_OFFSET);

            KafkaTestUtils.completeWork(wmm, work, 69);

            KafkaTestUtils.completeWork(wmm, work, 25_000);

            KafkaTestUtils.completeWork(wmm, work, highestSucceeded);


            // make the commit
            var completedEligibleOffsets = wmm.collectCommitDataForDirtyPartitions();
            assertThat(completedEligibleOffsets.get(tp).offset()).isEqualTo(FIRST_COMMITTED_OFFSET);
            consumerSpy.commitSync(completedEligibleOffsets);

            {
                // check for graceful fall back to the smallest available encoder
                OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(module);
                OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
                var state = wmm.getPm().getPartitionState(tp);
                String bestPayload = om.makeOffsetMetadataPayload(FIRST_COMMITTED_OFFSET, state);
                assertThat(bestPayload).isNotEmpty();
            }
        }

        // check
        {
            var committed = consumerSpy.committed(UniSets.of(tp)).get(tp);
            assertThat(committed.offset()).isEqualTo(FIRST_COMMITTED_OFFSET);

            if (assumeWorkingCodec(encoding, encodingsThatFail)) {
                assertThat(committed.metadata()).isNotBlank();
            }
        }

        // simulate a rebalance or some sort of reset, by instantiating a new WM with the state from the last

        // read offsets
        {
            final PCModule<String, String> moduleThree = new PCModule<>(options);
            var newWm = moduleThree.workManager();
            newWm.onPartitionsAssigned(UniSets.of(tp));

            //
            var pm = newWm.getPm();
            var partitionState = pm.getPartitionState(tp);

            if (assumeWorkingCodec(encoding, encodingsThatFail)) {
                // check state reloaded ok from consumer
                assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(highestSucceeded);
            }

            //
            ConsumerRecords<String, String> testRecordsWithBaseCommittedRecordRemoved = new ConsumerRecords<>(UniMaps.of(tp,
                    testRecords.records(tp)
                            .stream()
                            .filter(x ->
                                    x.offset() >= FIRST_COMMITTED_OFFSET)
                            .collect(Collectors.toList())));
            EpochAndRecordsMap<String, String> epochAndRecordsMap = new EpochAndRecordsMap<>(testRecordsWithBaseCommittedRecordRemoved, newWm.getPm());
            newWm.registerWork(epochAndRecordsMap);

            if (assumeWorkingCodec(encoding, encodingsThatFail)) {
                // check state reloaded ok from consumer
                assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(highestSucceeded);
            }

            //
            if (assumeWorkingCodec(encoding, encodingsThatFail)) {
                assertTruth(partitionState).getOffsetHighestSequentialSucceeded().isEqualTo(FIRST_SUCCEEDED_OFFSET);

                assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(highestSucceeded);

                long offsetHighestSeen = partitionState.getOffsetHighestSeen();
                assertThat(offsetHighestSeen).isEqualTo(highestSucceeded);

                var incompletes = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
                Truth.assertThat(incompletes).containsExactlyElementsIn(expected);
            }

            // check record is marked as incomplete
            var anIncompleteRecord = records.get(3);
            assertThat(partitionState.isRecordPreviouslyCompleted(anIncompleteRecord)).isFalse();

            // check state
            {
                if (assumeWorkingCodec(encoding, encodingsThatFail)) {
                    long offsetHighestSequentialSucceeded = partitionState.getOffsetHighestSequentialSucceeded();
                    assertThat(offsetHighestSequentialSucceeded).isEqualTo(0);

                    long offsetHighestSucceeded = partitionState.getOffsetHighestSucceeded();
                    assertThat(offsetHighestSucceeded).isEqualTo(highestSucceeded);

                    long offsetHighestSeen = partitionState.getOffsetHighestSeen();
                    assertThat(offsetHighestSeen).isEqualTo(highestSucceeded);

                    var incompletes = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
                    Truth.assertThat(incompletes).containsExactlyElementsIn(expected);

                    assertThat(partitionState.isRecordPreviouslyCompleted(anIncompleteRecord)).isFalse();
                }
            }


            var workRetrieved = newWm.getWorkIfAvailable();
            var workRetrievedOffsets = workRetrieved.stream().map(WorkContainer::offset).collect(Collectors.toList());
            assertTruth(workRetrieved).isNotEmpty();

            switch (encoding) {
                case BitSet, BitSetCompressed, // BitSetV1 both get a short overflow due to the length being too long
                        BitSetV2, // BitSetv2 uncompressed is too large to fit in metadata payload
                        RunLength, RunLengthCompressed // RunLength V1 max runlength is Short.MAX_VALUE
                        -> {
                    assertThat(workRetrievedOffsets).doesNotContain(2500L);
                    assertThat(workRetrievedOffsets).doesNotContainSequence(expected);
                }
                default -> {
                    Truth.assertWithMessage("Contains only incomplete records")
                            .that(workRetrievedOffsets)
                            .containsExactlyElementsIn(expected)
                            .inOrder();
                }
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * A {@link OffsetEncoding} that works in this test scenario
     */
    private boolean assumeWorkingCodec(OffsetEncoding encoding, List<OffsetEncoding> encodingsThatFail) {
        return !encodingsThatFail.contains(encoding);
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
        var incompletes = new TreeSet<>(UniSets.of(1L, 4L, 5L, 100L));

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

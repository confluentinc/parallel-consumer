package io.confluent.parallelconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.OffsetEncoding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class OffsetEncodingTests extends ParallelEoSStreamProcessorTestBase {

    @Test
    void runLengthDeserialise() {
        var sb = ByteBuffer.allocate(3);
        sb.put((byte) 0); // magic byte place holder, can ignore
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
    void largeIncompleteOffsetValues(long nextExpectedOffset) {
        var incompletes = new HashSet<Long>();
        long lowWaterMark = 123L;
        incompletes.addAll(UniSets.of(lowWaterMark, 2345L, 8765L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        encoder.compressionForced = true;

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
    void ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential(OffsetEncoding encoding) throws BrokenBarrierException, InterruptedException {
        assumeThat("Codec skipped, not applicable", encoding,
                not(in(of(ByteArray, ByteArrayCompressed)))); // byte array not currently used

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

        var firstSucceededRecordRemoved = new ArrayList<>(records);
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 0).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 69).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 25_000).findFirst().get());

        //
        ktu.send(consumerSpy, records);

        //
        ParallelConsumerOptions options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, records);
        ConsumerRecords<String, String> crs = new ConsumerRecords<>(recordsMap);

        // write offsets
        Map<TopicPartition, OffsetAndMetadata> completedEligibleOffsetsAndRemove;
        {
            WorkManager<String, String> wmm = new WorkManager<>(options, consumerSpy);
            wmm.registerWork(crs);

            List<WorkContainer<String, String>> work = wmm.maybeGetWork();
            assertThat(work).hasSameSizeAs(records);

            KafkaTestUtils.completeWork(wmm, work, 0);

            KafkaTestUtils.completeWork(wmm, work, 69);

            KafkaTestUtils.completeWork(wmm, work, 25_000);

            completedEligibleOffsetsAndRemove = wmm.findCompletedEligibleOffsetsAndRemove();

            // check for graceful fall back to the smallest available encoder
            OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(wmm, consumerSpy);
            Set<Long> collect = firstSucceededRecordRemoved.stream().map(x -> x.offset()).collect(Collectors.toSet());
            OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
            String bestPayload = om.makeOffsetMetadataPayload(1, tp, collect);
            assertThat(bestPayload).isNotEmpty();
        }
        consumerSpy.commitSync(completedEligibleOffsetsAndRemove);

        // read offsets
        {
            WorkManager<String, String> newWm = new WorkManager<>(options, consumerSpy);
            newWm.onPartitionsAssigned(UniSets.of(tp));
            newWm.registerWork(crs);
            List<WorkContainer<String, String>> workContainers = newWm.maybeGetWork();
            switch (encoding) {
                case BitSet, BitSetCompressed, // BitSetV1 both get a short overflow due to the length being too long
                        BitSetV2, // BitSetv2 uncompressed is too large to fit in metadata payload
                        RunLength, RunLengthCompressed // RunLength V1 max runlength is Short.MAX_VALUE
                        -> {
                    assertThatThrownBy(() ->
                            assertThat(workContainers).extracting(WorkContainer::getCr)
                                    .containsExactlyElementsOf(firstSucceededRecordRemoved))
                            .hasMessageContaining("but some elements were not expected")
                            .hasMessageContaining("offset = 25000");
                }
                default -> {
                    assertThat(workContainers).extracting(WorkContainer::getCr).containsExactlyElementsOf(firstSucceededRecordRemoved);
                }
            }
        }
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
    void ensureEncodingGracefullyWorksWhenOffsetsArentSequentialTwo() {
        long nextExpectedOffset = 101;
        long lowWaterMark = 0;
        var incompletes = new HashSet<>(UniSets.of(1L, 4L, 5L, 100L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        encoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);

        if (nextExpectedOffset - lowWaterMark > BitsetEncoder.MAX_LENGTH_ENCODABLE)
            assertThat(encodingMap.keySet()).as("Gracefully ignores that Bitset can't be supported").doesNotContain(OffsetEncoding.BitSet);
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
    }

}

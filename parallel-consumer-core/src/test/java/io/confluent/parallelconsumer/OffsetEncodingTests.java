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
    void largeIncompleteOffsetValues(long currentHighestCompleted) {
        var incompletes = new HashSet<Long>();
        long lowWaterMark = 123L;
        incompletes.addAll(UniSets.of(lowWaterMark, 2345L, 8765L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, currentHighestCompleted);
        encoder.compressionForced = true;

        //
//        encoder.runOverIncompletes(incompletes, lowWaterMark, currentHighestCompleted);
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetData unwrap = EncodedOffsetData.unwrap(smallestBytes);
        ParallelConsumer.Tuple<Long, Set<Long>> decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getRight()).containsExactlyInAnyOrderElementsOf(incompletes);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetData bitsetUnwrap = EncodedOffsetData.unwrap(encoder.packEncoding(new EncodedOffsetData(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                ParallelConsumer.Tuple<Long, Set<Long>> decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getRight())
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

        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 10, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 11, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 12, "akey", "avalue"));

        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 69, "akey", "avalue")); // will complete
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 100, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 1_000, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 20_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 25_000, "akey", "avalue")); // will complete, near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 30_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE

        // Extremely large tests for v2 encoders
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 40_000, "akey", "avalue")); // higher than Short.MAX_VALUE
        int avoidOffByOne = 2;
        int complete = 40_000 + Short.MAX_VALUE + avoidOffByOne;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, complete, "akey", "avalue")); // runlength higher than Short.MAX_VALUE // will complete

        int largeTwo = 40_000 + Short.MAX_VALUE + avoidOffByOne * 100;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, largeTwo, "akey", "avalue")); // runlength higher than Short.MAX_VALUE // incomplete (should be ignored)

//        int recsToRequestArbitrary = 10000;

        var firstSucceededRecordRemoved = new ArrayList<>(records);
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 0).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 4).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 10).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 11).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 12).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 69).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 25_000).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == complete).findFirst().get());

        //
        ktu.send(consumerSpy, records);

        // setup
        ParallelConsumerOptions options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, records);
        ConsumerRecords<String, String> crs = new ConsumerRecords<>(recordsMap);


        // write offsets
        Map<TopicPartition, OffsetAndMetadata> completedEligibleOffsetsAndRemove;
        {
            WorkManager<String, String> wmm = new WorkManager<>(options, consumerManager);
            wmm.onPartitionsAssigned(UniSets.of(new TopicPartition(INPUT_TOPIC, 0)));
            wmm.registerWork(crs);

            List<WorkContainer<String, String>> work = wmm.maybeGetWork();
            assertThat(work).hasSameSizeAs(records);

            KafkaTestUtils.completeWork(wmm, work, 0);

            KafkaTestUtils.completeWork(wmm, work, 4);

            KafkaTestUtils.completeWork(wmm, work, 10);
            KafkaTestUtils.completeWork(wmm, work, 11);
            KafkaTestUtils.completeWork(wmm, work, 12);

            KafkaTestUtils.completeWork(wmm, work, 69);

            KafkaTestUtils.completeWork(wmm, work, 25_000);

            KafkaTestUtils.completeWork(wmm, work, complete);

//            completedEligibleOffsetsAndRemove = wmm.findCompletedEligibleOffsetsAndRemove(); // old version
            completedEligibleOffsetsAndRemove = wmm.serialiseEncoders(); // new version
//            String bestPayload = topicPartitionOffsetAndMetadataMap.
            // check for graceful fall back to the smallest available encoder
//            OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(wmm, consumerManager);

//            Set<Long> collect = firstSucceededRecordRemoved.stream().map(x -> x.offset()).collect(Collectors.toSet());
            OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
//            String bestPayload = om.makeOffsetMetadataPayload(1, tp, collect);
//            assertThat(bestPayload).isNotEmpty();
        }
        consumerSpy.commitSync(completedEligibleOffsetsAndRemove);

        // read offsets
        {
            WorkManager<String, String> newWm = new WorkManager<>(options, consumerManager);
            newWm.onPartitionsAssigned(UniSets.of(tp));
            newWm.registerWork(crs);
            List<WorkContainer<String, String>> workContainers = newWm.maybeGetWork();
            switch (encoding) {
                case BitSet, BitSetCompressed, // BitSetV1 both get a short overflow due to the length being too long
                        BitSetV2, // BitSetv2 uncompressed is too large to fit in metadata payload, so the whole encoding is dropped ~9101 bytes (max ~4000)
                        RunLength, RunLengthCompressed // RunLength V1 max runlength is Short.MAX_VALUE
                        -> {
                    assertThatThrownBy(() ->
                            assertThat(workContainers).extracting(WorkContainer::getCr)
                                    .containsExactlyElementsOf(firstSucceededRecordRemoved))
                            .hasMessageContaining("were not expected")
                            .hasMessageContaining("offset = 20000");
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
        long currentHighestCompleted = 101;
        long lowWaterMark = 0;
        var incompletes = new HashSet<>(UniSets.of(1L, 4L, 5L, 100L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, currentHighestCompleted);
        encoder.compressionForced = true;

        //
//        encoder.runOverIncompletes(incompletes, lowWaterMark, currentHighestCompleted);
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetData unwrap = EncodedOffsetData.unwrap(smallestBytes);
        ParallelConsumer.Tuple<Long, Set<Long>> decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getRight()).containsExactlyInAnyOrderElementsOf(incompletes);

        if (currentHighestCompleted - lowWaterMark > BitsetEncoder.MAX_LENGTH_ENCODABLE)
            assertThat(encodingMap.keySet()).as("Gracefully ignores that Bitset can't be supported").doesNotContain(OffsetEncoding.BitSet);
        else
            assertThat(encodingMap.keySet()).contains(OffsetEncoding.BitSet);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetData bitsetUnwrap = EncodedOffsetData.unwrap(encoder.packEncoding(new EncodedOffsetData(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                ParallelConsumer.Tuple<Long, Set<Long>> decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getRight())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }
    }

}

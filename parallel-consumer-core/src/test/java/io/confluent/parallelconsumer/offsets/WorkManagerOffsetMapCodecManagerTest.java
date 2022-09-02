package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xerial.snappy.SnappyOutputStream;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.offsets.OffsetCodecTestUtils.bitmapStringToIncomplete;
import static io.confluent.parallelconsumer.offsets.OffsetCodecTestUtils.incompletesToBitmapString;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;

// todo refactor - remove tests which use hard coded state vs dynamic state - #compressionCycle, #selialiseCycle, #runLengthEncoding, #loadCompressedRunLengthRncoding
@Slf4j
@ExtendWith(MockitoExtension.class)
class WorkManagerOffsetMapCodecManagerTest {

    WorkManager<String, String> wm;

    OffsetMapCodecManager<String, String> offsetCodecManager;

    TopicPartition tp = new TopicPartition("myTopic", 0);

    /**
     * set pf incomplete offsets in our sample data
     */
    TreeSet<Long> incompleteOffsets = new TreeSet<>(UniSets.of(0L, 2L, 3L));

    /**
     * Committable offset of 0, meaning 1 and 4 are complete and 2 and 3 are incomplete
     * <p>
     * 0X00X
     */
    long finalOffsetForPartition = 0L;

    /**
     * Sample data runs up to a highest seen offset of 4. Where offset 3 and 3 are incomplete.
     */
    long highestSucceeded = 4;

    PartitionState<String, String> state = new PartitionState<>(tp, new OffsetMapCodecManager.HighestOffsetAndIncompletes(of(highestSucceeded), incompleteOffsets));

    @Mock
    ConsumerRecord<String, String> mockCr;

    @BeforeEach
    void setupMock() {
        injectSucceededWorkAtOffset(highestSucceeded);
    }

    private void injectSucceededWorkAtOffset(long offset) {
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, null, TimeUtils.getClock());
        Mockito.doReturn(offset).when(mockCr).offset();
        state.addWorkContainer(workContainer);
        state.onSuccess(workContainer); // in this case the highest seen is also the highest succeeded
    }

    /**
     * o = incomplete x = complete
     */
    static List<String> simpleSampleInputsToCompress = UniLists.of(
            "",
            "o",
            "x",
            "ooo",
            "xxx",
            "xox",
            "oxo",
            "xooxo",
            "ooxxoxox",
            "xxxxxxoooooxoxoxoooooxxxxooooo",
            "oooooooooooooooooooooooooooooo",
            "ooooooooooooooxxxxxxxxxxxxxxxx",
            "oxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo"
    );

    @Getter
    static List<String> inputsToCompress = new ArrayList<>();

    @BeforeEach
    void setup() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .build();
        wm = new WorkManager<>(new PCModule<>(options));
        wm.onPartitionsAssigned(UniLists.of(tp));
        offsetCodecManager = new OffsetMapCodecManager<>(mockConsumer);
    }

    @BeforeAll
    static void data() {
        String input100 = "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo"; //100 chars

        StringBuffer randomInput = generateRandomData(100);
        String inputString = randomInput.toString();

        inputsToCompress.addAll(simpleSampleInputsToCompress);
        inputsToCompress.add(input100);
        inputsToCompress.add(input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100);
        inputsToCompress.add(inputString);
        inputsToCompress.add(generateRandomData(1000).toString());
        // remove? slow, not needed?
        inputsToCompress.add(generateRandomData(10000).toString());
        inputsToCompress.add(generateRandomData(30000).toString());
    }

    private static StringBuffer generateRandomData(int entries) {
        StringBuffer randomInput = new StringBuffer();
        range(entries).toStream()
                .mapToObj(x -> RandomUtils.nextBoolean())
                .forEach(x -> randomInput.append((x) ? 'x' : 'o'));
        return randomInput;
    }

    @SneakyThrows
    @Test
    void serialiseCycle() {
        String serialised = offsetCodecManager.serialiseIncompleteOffsetMapToBase64(finalOffsetForPartition, state);
        log.info("Size: {}", serialised.length());

        //
        OffsetMapCodecManager.HighestOffsetAndIncompletes highestOffsetAndIncompletes = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(finalOffsetForPartition, serialised);
        Set<Long> deserializedIncompletes = highestOffsetAndIncompletes.getIncompleteOffsets();

        //
        assertThat(deserializedIncompletes.toArray()).containsExactly(incompleteOffsets.toArray());
    }

    /**
     * Even Java _binary_ serialisation has very large overheads.
     */
    @Test
    void javaSerialisationComparison() {
        TreeSet<Long> one = new TreeSet<>(UniSets.of(1L));
        TreeSet<Long> two = new TreeSet<>(UniSets.of(2L));

        String oneS = OffsetSimpleSerialisation.encodeAsJavaObjectStream(one);
        int payloadLength = 5;
        String oneStringPreamble = oneS.substring(0, oneS.length() - payloadLength);
        String twoS = OffsetSimpleSerialisation.encodeAsJavaObjectStream(two);
        String twoStringPreamble = twoS.substring(0, twoS.length() - payloadLength);

        assertThat(oneStringPreamble).isEqualTo(twoStringPreamble);
    }

    @SneakyThrows
    @Test
    void runLengthEncodingCompression() {
        List<String> inputs = UniLists.of(
                "xxxxxxoooooxoxoxoooooxxxxooooo",
                "6x,5o,1x,1o,1x,1o,1x,5o,4x,5o",
                "oooooooooooooooooooooooooooooo",
                "30o",
                "ooooooooooooooxxxxxxxxxxxxxxxx",
                "15o,15x",
                "x",
                "1x",
                "");

        for (var i : inputs) {
            compareCompression(i);
        }
    }

    private byte[] compareCompression(String input) throws IOException {
        log.info("testing input of {}", input);

        byte[] inputBytes = input.getBytes(UTF_8);

        byte[] outg = OffsetSimpleSerialisation.compressGzip(inputBytes);
        byte[] outz = OffsetSimpleSerialisation.compressZstd(inputBytes);
        ByteArrayOutputStream outs = new ByteArrayOutputStream();

        var snap = new SnappyOutputStream(outs);

        snap.write(inputBytes);

        snap.close();

        String g64 = Base64.getEncoder().encodeToString(outg);
        String z64 = Base64.getEncoder().encodeToString(outz);
        String s64 = Base64.getEncoder().encodeToString(outs.toByteArray());

        String raw64 = Base64.getEncoder().encodeToString(inputBytes);

        log.info("g {}", outg.length);
        log.info("z {}", outz.length);
        log.info("s {}", outs.size());

        log.info("64");
        log.info("r {}", raw64.length());
        log.info("g {}", g64.length());
        log.info("z {}", z64.length());
        log.info("s {}", s64.length());

        return outg;
    }

    @Test
    @Disabled("TODO: Blocker: Not implemented yet")
    void truncationOnCommit() {
        wm.onOffsetCommitSuccess(UniMaps.of());
        assertThat(true).isFalse();
    }

    @Test
    void base64Encoding() {
        // encode
        String originalString = "TEST";
        byte[] stringBytes = originalString.getBytes(UTF_8);
        String base64Bytes = Base64.getEncoder().encodeToString(stringBytes);

        // decode
        byte[] base64DecodedBytes = Base64.getDecoder().decode(base64Bytes);
        assertThat(stringBytes).isEqualTo(base64DecodedBytes);

        // string
        String decodedString = new String(base64DecodedBytes, UTF_8);
        assertThat(originalString).isEqualTo(decodedString);
    }

    @SneakyThrows
    @Test
    void loadCompressedRunLengthEncoding() {
        byte[] bytes = offsetCodecManager.encodeOffsetsCompressed(finalOffsetForPartition, state);
        OffsetMapCodecManager.HighestOffsetAndIncompletes longs = OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, bytes);
        assertThat(longs.getIncompleteOffsets().toArray()).containsExactly(incompleteOffsets.toArray());
    }

    @Test
    void decodeOffsetMap() {
        Set<Long> set = bitmapStringToIncomplete(2L, "ooxx");
        assertThat(set).containsExactly(2L, 3L);

        assertThat(bitmapStringToIncomplete(2L, "ooxxoxox")).containsExactly(2L, 3L, 6L, 8L);
        assertThat(bitmapStringToIncomplete(2L, "o")).containsExactly(2L);
        assertThat(bitmapStringToIncomplete(2L, "x")).containsExactly();
        assertThat(bitmapStringToIncomplete(2L, "")).containsExactly();
        assertThat(bitmapStringToIncomplete(2L, "ooo")).containsExactly(2L, 3L, 4L);
        assertThat(bitmapStringToIncomplete(2L, "xxx")).containsExactly();
    }

    @Test
    void binaryArrayConstruction() {
        injectSucceededWorkAtOffset(6);

        String encoding = incompletesToBitmapString(finalOffsetForPartition, state);
        assertThat(encoding).isEqualTo("oxooxx");
    }

    @SneakyThrows
    @Test
    void compressDecompressSanityGzip() {
        final byte[] input = "Lilan".getBytes();
        final var compressedInput = OffsetSimpleSerialisation.compressGzip(input);
        final var decompressedInput = OffsetSimpleSerialisation.decompressGzip(ByteBuffer.wrap(compressedInput));
        assertThat(decompressedInput).isEqualTo(input);
    }

    @SneakyThrows
    @Test
    void compressDecompressWithBase64SanityGzip() {
        byte[] input = "Lilan".getBytes();
        byte[] compressedInput = OffsetSimpleSerialisation.compressGzip(input);
        byte[] b64input = Base64.getEncoder().encode(compressedInput);
        byte[] b64Output = Base64.getDecoder().decode(b64input);
        byte[] decompressedInput = OffsetSimpleSerialisation.decompressGzip(ByteBuffer.wrap(b64Output));
        assertThat(decompressedInput).isEqualTo(input);
    }

    @SneakyThrows
    @Test
    void compressDecompressSanityZstd() {
        byte[] input = "Lilan".getBytes();
        byte[] compressedInput = OffsetSimpleSerialisation.compressZstd(input);
        ByteBuffer decompressedInput = OffsetSimpleSerialisation.decompressZstd(ByteBuffer.wrap(compressedInput));
        assertThat(decompressedInput).isEqualTo(ByteBuffer.wrap(input));
    }

    @SneakyThrows
    @Test
    void largeOffsetMap() {
        injectSucceededWorkAtOffset(200); // force system to have seen a high offset
        byte[] bytes = offsetCodecManager.encodeOffsetsCompressed(0L, state);
        int smallestCompressionObserved = 10;
        assertThat(bytes).as("very small")
                .hasSizeLessThan(smallestCompressionObserved); // arbitrary size expectation based on past observations - expect around 7
    }

    @SneakyThrows
    @Test
    void stringVsByteVsBitSetEncoding() {
        for (var inputString : inputsToCompress) {
            int inputLength = inputString.length();

            Set<Long> longs = bitmapStringToIncomplete(finalOffsetForPartition, inputString);

            OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highestSucceeded, longs).invoke();
            byte[] byteByte = simultaneousEncoder.getEncodingMap().get(ByteArray);
            byte[] bitsBytes = simultaneousEncoder.getEncodingMap().get(BitSet);

//            int compressedBytes = om.compressZstd(byteByte).length;
//            int compressedBits = om.compressZstd(bitsBytes).length;

            byte[] runlengthBytes = simultaneousEncoder.getEncodingMap().get(RunLength);
//            int rlBytesCompressed = om.compressZstd(runlengthBytes).length;

            log.info("in: {}", inputString);
//            log.info("length: {} comp bytes: {} comp bits: {}, uncompressed bits: {}, run length {}, run length compressed: {}", inputLength, compressedBytes, compressedBits, bitsBytes.length, runlengthBytes.length, rlBytesCompressed);
        }
    }

    @SneakyThrows
    @Test
    void deserialiseBitSet() {
        var input = "oxxooooooo";
        long highestSucceeded = input.length() - 1;

        int nextExpectedOffset = 0;
        Set<Long> incompletes = bitmapStringToIncomplete(nextExpectedOffset, input);
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(nextExpectedOffset, highestSucceeded, incompletes);
        encoder.invoke();
        byte[] pack = encoder.packSmallest();

        //
        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(pack);
        String deserialisedBitSet = encodedOffsetPair.getDecodedString();
        assertThat(deserialisedBitSet).isEqualTo(input);
    }

    @SneakyThrows
    @Test
    void compressionCycle() {
        byte[] serialised = offsetCodecManager.encodeOffsetsCompressed(finalOffsetForPartition, state);

        OffsetMapCodecManager.HighestOffsetAndIncompletes deserialised = OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, serialised);

        assertThat(deserialised.getIncompleteOffsets()).isEqualTo(incompleteOffsets);
    }

    @Test
    void runLengthEncoding() {
        String stringMap = incompletesToBitmapString(finalOffsetForPartition, state);
        List<Integer> integers = OffsetRunLength.runLengthEncode(stringMap);
        assertThat(integers).as("encoding of map: " + stringMap).containsExactlyElementsOf(UniLists.of(1, 1, 2));

        assertThat(OffsetRunLength.runLengthDecodeToString(integers)).isEqualTo(stringMap);
    }

    static List<String> differentInputsAndCompressions() {
        return inputsToCompress;
    }

    /**
     * Compare compression performance on different types of inputs, and tests that each encoding type is decompressed
     * again correctly
     */
    @ParameterizedTest
    @MethodSource
    void differentInputsAndCompressions(String input) {
        long highestSeen = input.length() - 1; // pretend we've gone one higher than the input incompletes

        //
        log.debug("Testing round - size: {} input: '{}'", input.length(), input);
        Set<Long> inputIncompletes = bitmapStringToIncomplete(finalOffsetForPartition, input);
        String sanityEncoding = incompletesToBitmapString(finalOffsetForPartition, highestSeen + 1, inputIncompletes);
        Truth.assertThat(sanityEncoding).isEqualTo(input);

        //
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highestSeen, inputIncompletes);
        encoder.invoke();

        // test all encodings created
        for (final EncodedOffsetPair encoding : encoder.sortedEncodings) {
            //
            byte[] packedEncoding = encoder.packEncoding(encoding);

            //
            var recoveredIncompleteAndOffset =
                    OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, packedEncoding);
            Set<Long> recoveredIncompletes = recoveredIncompleteAndOffset.getIncompleteOffsets();

            //
            assertThat(recoveredIncompletes).containsExactlyInAnyOrderElementsOf(inputIncompletes);

            //
            String simple = incompletesToBitmapString(finalOffsetForPartition, highestSeen + 1, recoveredIncompletes);
            assertWithMessage(encoding.encoding.name())
                    .that(simple).isEqualTo(input);
        }
    }

}

package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.SnappyOutputStream;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static io.confluent.csid.asyncconsumer.OffsetEncoding.*;
import static io.confluent.csid.utils.Range.range;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO: use compressed avro instead for a more reliable long term schema system? or string encoding with a version
 *  prefix fine?
 */
@Slf4j
class WorkManagerOffsetMapCodecManagerTest {

    WorkManager<String, String> wm;

    OffsetMapCodecManager<String, String> om;

    TopicPartition tp = new TopicPartition("myTopic", 0);

    TreeSet<Long> incomplete = new TreeSet<>(UniSets.of(2L, 3L));

    long finalOffsetForPartition = 0;
    long partitionHighWaterMark = 4;

    static List<String> simpleInputs = UniLists.of(
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

    static List<String> inputs = new ArrayList<String>();

    @BeforeEach
    void setup() {
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        wm = new WorkManager<>(AsyncConsumerOptions.builder().build(), consumer);
        om = new OffsetMapCodecManager(wm, consumer);
        wm.partitionOffsetHighWaterMarks.put(tp, partitionHighWaterMark);
    }

    @BeforeAll
    static void data() {
        String input100 = "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo"; //100 chars

        StringBuffer randomInput = generateRandomData(100);
        String inputString = randomInput.toString();

        inputs.addAll(simpleInputs);
        inputs.add(input100);
        inputs.add(input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100);
        inputs.add(inputString);
        inputs.add(generateRandomData(1000).toString());
        inputs.add(generateRandomData(10000).toString());
        inputs.add(generateRandomData(30000).toString());
    }

    @NotNull
    private static StringBuffer generateRandomData(int entries) {
        StringBuffer randomInput = new StringBuffer();
        range(entries).toStream()
                .mapToObj(x -> RandomUtils.nextBoolean())
                .forEach(x -> randomInput.append((x) ? 'x' : 'o'));
//                .forEach(x -> randomInput.append('x'));
//        randomInput.setCharAt(1354, 'o');
        return randomInput;
    }

    @Test
    void serialiseCycle() {
        String serialised = om.serialiseIncompleteOffsetMapToBase64(finalOffsetForPartition, tp, incomplete);
        log.info("Size: {}", serialised.length());

        //
        Set<Long> longs = om.deserialiseIncompleteOffsetMapFromBase64(finalOffsetForPartition, serialised).getRight();

        //
        assertThat(longs.toArray()).containsExactly(incomplete.toArray());
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

    @Test
    void loadCompressedRunLengthEncoding() {
        byte[] bytes = om.encodeOffsetsCompressed(finalOffsetForPartition, tp, incomplete);
        AsyncConsumer.Tuple<Long, Set<Long>> longs = om.decodeCompressedOffsets(finalOffsetForPartition, bytes);
        assertThat(longs.getRight().toArray()).containsExactly(incomplete.toArray());
    }

    @Test
    void decodeOffsetMap() {
        Set<Long> set = om.bitmapStringToIncomplete(2L, "ooxx");
        assertThat(set).containsExactly(2L, 3L);

        assertThat(om.bitmapStringToIncomplete(2L, "ooxxoxox")).containsExactly(2L, 3L, 6L, 8L);
        assertThat(om.bitmapStringToIncomplete(2L, "o")).containsExactly(2L);
        assertThat(om.bitmapStringToIncomplete(2L, "x")).containsExactly();
        assertThat(om.bitmapStringToIncomplete(2L, "")).containsExactly();
        assertThat(om.bitmapStringToIncomplete(2L, "ooo")).containsExactly(2L, 3L, 4L);
        assertThat(om.bitmapStringToIncomplete(2L, "xxx")).containsExactly();
    }

    @Test
    void binaryArrayConstruction() {
        wm.raisePartitionHighWaterMark(6L, tp);
        String s = om.incompletesToBitmapString(1L, tp, incomplete); //2,3
        assertThat(s).isEqualTo("xooxx");
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

    @Test
    void largeOffsetMap() {
        wm.raisePartitionHighWaterMark(200L, tp);
        byte[] bytes = om.encodeOffsetsCompressed(0L, tp, incomplete);
        assertThat(bytes.length).as("very small").isLessThan(30);
    }

    @SneakyThrows
    @Test
    void stringVsByteVsBitSetEncoding() {
        for (var inputString : inputs) {
            int inputLength = inputString.length();

            Set<Long> longs = om.bitmapStringToIncomplete(finalOffsetForPartition, inputString);

            OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, partitionHighWaterMark, longs).invoke();
            byte[] byteByte = simultaneousEncoder.getEncodingMap().get(ByteArray);
            byte[] bitsBytes = simultaneousEncoder.getEncodingMap().get(BitSet);

//            int compressedBytes = om.compressZstd(byteByte).length;
//            int compressedBits = om.compressZstd(bitsBytes).length;

            byte[] runlengthBytes = simultaneousEncoder.getEncodingMap().get(RunLength);
//            int rlBytesCompressed = om.compressZstd(runlengthBytes).length;

            log.info("in: {}", inputString);
//            log.info("length: {} comp bytes: {} comp bits: {}, uncompressed bits: {}, run length {}, run length compressed: {}", inputLength, compressedBytes, compressedBits, bitsBytes.length, runlengthBytes.length, rlBytesCompressed);
        }
        return; // breakpoint
    }

    @SneakyThrows
    @Test
    void deserialiseBitset() {
        var input = "oxxooooooo";
        long highWater = input.length();
        wm.raisePartitionHighWaterMark(highWater, tp);

        Set<Long> longs = om.bitmapStringToIncomplete(finalOffsetForPartition, input);
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highWater, longs);
        encoder.invoke();
        byte[] pack = encoder.packSmallest();

        //
        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(pack);
        String deserialisedBitSet = encodedOffsetPair.getDecodedString();
        assertThat(deserialisedBitSet).isEqualTo(input);
    }

    @Test
    void compressionCycle() {
        byte[] serialised = om.encodeOffsetsCompressed(finalOffsetForPartition, tp, incomplete);

        AsyncConsumer.Tuple<Long, Set<Long>> deserialised = om.decodeCompressedOffsets(finalOffsetForPartition, serialised);

        assertThat(deserialised.getRight()).isEqualTo(incomplete);
    }

    @Test
    void runLengthEncoding() {
        String stringMap = om.incompletesToBitmapString(finalOffsetForPartition, tp, incomplete);
        List<Integer> integers = OffsetRunLength.runLengthEncode(stringMap);
        assertThat(integers).as("encoding of map: " + stringMap).containsExactlyElementsOf(UniLists.of(0, 2, 2));

        assertThat(OffsetRunLength.runLengthDecodeToString(integers)).isEqualTo(stringMap);
    }

    @Test
    void differentInputs() {
        for (final String input : inputs) {
            // override high water mark setup, as the test sets it manually
            setup();
            wm.partitionOffsetHighWaterMarks.put(tp, 0L); // hard reset to zero
            long highWater = input.length();
            wm.raisePartitionHighWaterMark(highWater, tp);

            //
            log.debug("Testing round - size: {} input: '{}'", input.length(), input);
            Set<Long> longs = om.bitmapStringToIncomplete(finalOffsetForPartition, input);
            OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highWater, longs);
            encoder.invoke();

            // test all encodings created
            for (final EncodedOffsetPair pair : encoder.sortedEncodings) {
                byte[] result = encoder.packEncoding(pair);

                //
                AsyncConsumer.Tuple<Long, Set<Long>> recoveredIncompleteOffsetTuple = om.decodeCompressedOffsets(finalOffsetForPartition, result);
                Set<Long> recoveredIncompletes = recoveredIncompleteOffsetTuple.getRight();

                //
                assertThat(recoveredIncompletes).containsExactlyInAnyOrderElementsOf(longs);

                //
                String recoveredOffsetBitmapAsString = om.incompletesToBitmapString(finalOffsetForPartition, tp, recoveredIncompletes);
                assertThat(recoveredOffsetBitmapAsString).isEqualTo(input);
            }
        }
    }

    @Disabled
    @Test
    void testAllInputsEachEncoding() {
        assertThat(true).isFalse();
    }

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

    @Test
    void largeIncompleteOffsetValues() {
        var incompletes = new HashSet<Long>();
        long lowWaterMark = 123L;
        incompletes.addAll(UniSets.of(lowWaterMark, 2345L, 8765L));
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, 10_000L, incompletes);
        encoder.invoke();
        byte[] bytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(bytes);
        AsyncConsumer.Tuple<Long, Set<Long>> decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getRight()).containsExactlyInAnyOrderElementsOf(incompletes);
    }

}
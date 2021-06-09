package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.confluent.csid.utils.BackportUtils.readFully;

@UtilityClass
@Slf4j
public class OffsetSimpleSerialisation {

    @SneakyThrows
    static String encodeAsJavaObjectStream(final Set<Long> incompleteOffsets) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(incompleteOffsets);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    private static TreeSet<Long> deserialiseJavaWriteObject(final byte[] decode) throws IOException, ClassNotFoundException {
        final Set<Long> raw;
        try (final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(decode))) {
            raw = (Set<Long>) objectInputStream.readObject();
        }
        return new TreeSet<>(raw);
    }

    @SneakyThrows
    static byte[] compressSnappy(final byte[] bytes) {
        try (final var out = new ByteArrayOutputStream();
             final var stream = new SnappyOutputStream(out)) {
            stream.write(bytes);
            return out.toByteArray();
        }
    }

    static ByteBuffer decompressSnappy(final ByteBuffer input) throws IOException {
        try (final var snappy = new SnappyInputStream(new ByteBufferInputStream(input))) {
            byte[] bytes = readFully(snappy);
            return ByteBuffer.wrap(bytes);
        }
    }

    static String base64(final ByteArrayOutputStream out) {
        final byte[] src = out.toByteArray();
        return base64(src);
    }

    static byte[] compressZstd(final byte[] bytes) throws IOException {
        final var out = new ByteArrayOutputStream();
        try (final var zstream = new ZstdOutputStream(out)) {
            zstream.write(bytes);
        }
        return out.toByteArray();
    }

    static byte[] compressGzip(final byte[] bytes) throws IOException {
        final var out = new ByteArrayOutputStream();
        try (final var zstream = new GZIPOutputStream(out)) {
            zstream.write(bytes);
        }
        return out.toByteArray();
    }

    static String base64(final byte[] src) {
        final byte[] encode = Base64.getEncoder().encode(src);
        final String out = new String(encode, OffsetMapCodecManager.CHARSET_TO_USE);
        log.trace("Final b64 size: {}", out.length());
        return out;
    }

    static byte[] decodeBase64(final String b64) {
        final byte[] bytes = b64.getBytes(OffsetMapCodecManager.CHARSET_TO_USE);
        return Base64.getDecoder().decode(bytes);
    }

    static ByteBuffer decompressZstd(final ByteBuffer input) throws IOException {
        try (final var zstream = new ZstdInputStream(new ByteBufferInputStream(input))) {
            final byte[] bytes = readFully(zstream);
            return ByteBuffer.wrap(bytes);
        }
    }

    static byte[] decompressGzip(final ByteBuffer input) throws IOException {
        try (final var gstream = new GZIPInputStream(new ByteBufferInputStream(input))) {
            return readFully(gstream);
        }
    }

    /**
     * @see OffsetEncoding#ByteArray
     */
    static String deserialiseByteArrayToBitMapString(final ByteBuffer data) {
        data.rewind();
        final StringBuilder sb = new StringBuilder(data.capacity());
        while (data.hasRemaining()) {
            final byte b = data.get();
            if (b == 1) {
                sb.append('x');
            } else {
                sb.append('o');
            }
        }
        return sb.toString();
    }
}

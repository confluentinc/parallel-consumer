package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

public class BackportUtils {

    /**
     * @see Duration#toSeconds() intro'd in Java 9 (isn't in 8)
     */
    public static long toSeconds(Duration duration) {
        return duration.toMillis() / 1000;
    }

    /**
     * @see Optional#isEmpty()  intro'd java 11
     */
    public static boolean isEmpty(Optional<?> optional) {
        return !optional.isPresent();
    }


    /**
     * @see Optional#isEmpty()  intro'd java 11
     */
    public static boolean hasNo(Optional<?> optional) {
        return !optional.isPresent();
    }

    public static byte[] readFully(InputStream is) throws IOException {
        return BackportUtils.readFully(is, -1, true);
    }

    /**
     * Used in Java 8 environments (Java 9 has read all bytes)
     * <p>
     * https://stackoverflow.com/a/25892791/105741
     */
    public static byte[] readFully(InputStream is, int length, boolean readAll) throws IOException {
        byte[] output = {};
        if (length == -1) length = Integer.MAX_VALUE;
        int pos = 0;
        while (pos < length) {
            int bytesToRead;
            if (pos >= output.length) { // Only expand when there's no room
                bytesToRead = Math.min(length - pos, output.length + 1024);
                if (output.length < pos + bytesToRead) {
                    output = Arrays.copyOf(output, pos + bytesToRead);
                }
            } else {
                bytesToRead = output.length - pos;
            }
            int cc = is.read(output, pos, bytesToRead);
            if (cc < 0) {
                if (readAll && length != Integer.MAX_VALUE) {
                    throw new EOFException("Detect premature EOF");
                } else {
                    if (output.length != pos) {
                        output = Arrays.copyOf(output, pos);
                    }
                    break;
                }
            }
            pos += cc;
        }
        return output;
    }

}

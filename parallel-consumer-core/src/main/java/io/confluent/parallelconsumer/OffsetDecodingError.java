package io.confluent.parallelconsumer;

public class OffsetDecodingError extends Exception {
    public OffsetDecodingError(final String s, final IllegalArgumentException a) {
        super(s, a);
    }
}

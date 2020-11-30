package io.confluent.parallelconsumer;

public class EncodingNotSupportedException extends Exception {
    public EncodingNotSupportedException(final String message) {
        super(message);
    }
}

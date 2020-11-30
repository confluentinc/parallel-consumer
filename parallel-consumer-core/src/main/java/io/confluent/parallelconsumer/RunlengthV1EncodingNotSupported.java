package io.confluent.parallelconsumer;

public class RunlengthV1EncodingNotSupported extends EncodingNotSupportedException {
    public RunlengthV1EncodingNotSupported(final String msg) {
        super(msg);
    }
}

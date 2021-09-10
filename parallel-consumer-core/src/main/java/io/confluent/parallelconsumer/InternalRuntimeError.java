package io.confluent.parallelconsumer;

public class InternalRuntimeError extends RuntimeException {

    public InternalRuntimeError(final String message) {
        super(message);
    }

    public InternalRuntimeError(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InternalRuntimeError(final Throwable cause) {
        super(cause);
    }

    public InternalRuntimeError(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

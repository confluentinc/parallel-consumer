package io.confluent.parallelconsumer;

public class InternalError extends RuntimeException {

    public InternalError(final String message) {
        super(message);
    }

    public InternalError(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InternalError(final Throwable cause) {
        super(cause);
    }

    public InternalError(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

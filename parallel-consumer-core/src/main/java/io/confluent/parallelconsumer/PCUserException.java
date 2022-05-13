package io.confluent.parallelconsumer;

/**
 * todo
 */
public class PCUserException extends RuntimeException {
    public PCUserException(String message) {
        super(message);
    }

    public PCUserException(String message, Throwable cause) {
        super(message, cause);
    }

    public PCUserException(Throwable cause) {
        super(cause);
    }
}

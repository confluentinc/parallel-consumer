package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;

import java.util.List;
import java.util.Optional;

/**
 * A user's processing function can throw this exception, which signals to PC that processing of the message has failed,
 * and that it should be retired at a later time.
 * <p>
 * The advantage of throwing this exception explicitly, is that PC will not log an ERROR. If any other type of exception
 * is thrown by the user's function, that will be logged as an error (but will still be retried later).
 * <p>
 * So in short, if this exception is thrown, nothing will be logged (except at DEBUG level), any other exception will be
 * logged as an error.
 */
public class PCRetriableException extends PCUserException {

    @Getter
    private Optional<Offsets> offsetsOptional = Optional.empty();

    /**
     * todo
     *
     * @param message
     * @param offsets
     */
    public PCRetriableException(String message, Offsets offsets) {
        super(message);
        this.offsetsOptional = Optional.of(offsets);
    }

    /**
     * todo
     *
     * @param message
     * @param offsets
     */
    public PCRetriableException(String message, List<Long> offsets) {
        super(message);
        offsetsOptional = Optional.empty();
    }

    public PCRetriableException(String message) {
        super(message);
    }

    public PCRetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public PCRetriableException(Throwable cause) {
        super(cause);
    }
}

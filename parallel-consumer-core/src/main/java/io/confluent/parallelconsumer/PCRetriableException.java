package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * A user's processing function can throw this exception, which signals to PC that processing of the message has failed,
 * and that it should be retired at a later time.
 * <p>
 * The advantage of throwing this exception explicitly, is that PC will not log an ERROR. If any other type of exception
 * is thrown by the user's function, that will be logged as an error (but will still be retried later).
 * <p>
 * So in short, if this exception is thrown, nothing will be logged (except at DEBUG level), any other exception will be
 * logged as an error.
 *
 * @author Antony Stubbs
 */
@StandardException
public class PCRetriableException extends RuntimeException {
}

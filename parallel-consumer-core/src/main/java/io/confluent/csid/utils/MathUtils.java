package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * @author Antony Stubbs
 */
public class MathUtils {

    /**
     * Ensures exact conversion from a Long to a Short.
     * <p>
     * {@link Math} doesn't have an exact conversion from Long to Short.
     *
     * @see Math#toIntExact
     */
    public static short toShortExact(long value) {
        if ((short) value != value) {
            throw new ArithmeticException("short overflow");
        }
        return (short) value;
    }
}

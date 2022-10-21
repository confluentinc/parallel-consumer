package io.confluent.csid.utils;

import lombok.experimental.UtilityClass;

/**
 * @author Antony Stubbs
 */
@UtilityClass
public class MathUtils {

    /**
     * Ensures exact conversion from a Long to a Short.
     * <p>
     * {@link Math} doesn't have an exact conversion from Long to Short.
     *
     * @see Math#toIntExact
     */
    public static short toShortExact(long value) {
        final short shortCast = (short) value;
        if (shortCast != value) {
            throw new ArithmeticException("short overflow");
        }
        return shortCast;
    }
}

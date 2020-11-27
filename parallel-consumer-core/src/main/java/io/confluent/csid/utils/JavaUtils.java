package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JavaUtils {
    public static int safeCast(final long aLong) {
        return Math.toIntExact(aLong);
    }
}

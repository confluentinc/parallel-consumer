package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import lombok.experimental.UtilityClass;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.time.Duration.ofMillis;

@UtilityClass
public class JavaUtils {

    public static <T> Optional<T> getLast(final List<T> commitHistory) {
        if (commitHistory.isEmpty()) return Optional.empty();
        return Optional.of(commitHistory.get(commitHistory.size() - 1));
    }

    public static <T> Optional<T> getOnlyOne(final Map<String, T> stringMapMap) {
        if (stringMapMap.isEmpty()) return Optional.empty();
        Collection<T> values = stringMapMap.values();
        if (values.size() > 1) throw new InternalRuntimeError("More than one element");
        return Optional.of(values.iterator().next());
    }

    public static Duration max(Duration left, Duration right) {
        long expectedDurationOfClose = Math.max(left.toMillis(), right.toMillis());
        return ofMillis(expectedDurationOfClose);
    }

    public static boolean isGreaterThan(Duration compare, Duration to) {
        return compare.compareTo(to) > 0;
    }

}

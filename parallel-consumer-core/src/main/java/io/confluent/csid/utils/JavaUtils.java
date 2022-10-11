package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.experimental.UtilityClass;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;

@UtilityClass
public class JavaUtils {

    public static <T> Optional<T> getLast(final List<T> someList) {
        if (someList.isEmpty()) return Optional.empty();
        return Optional.of(someList.get(someList.size() - 1));
    }

    public static <T> Optional<T> getFirst(final List<T> someList) {
        return someList.isEmpty() ? Optional.empty() : Optional.of(someList.get(0));
    }

    public static <T> Optional<T> getOnlyOne(final Map<String, T> stringMapMap) {
        if (stringMapMap.isEmpty()) return Optional.empty();
        Collection<T> values = stringMapMap.values();
        if (values.size() > 1) throw new InternalRuntimeException("More than one element");
        return Optional.of(values.iterator().next());
    }

    public static Duration max(Duration left, Duration right) {
        long expectedDurationOfClose = Math.max(left.toMillis(), right.toMillis());
        return ofMillis(expectedDurationOfClose);
    }

    public static boolean isGreaterThan(Duration compare, Duration to) {
        return compare.compareTo(to) > 0;
    }

    /**
     * A shortcut for changing only the values of a Map.
     * <p>
     * https://stackoverflow.com/a/50740570/105741
     */
    public static <K, V1, V2> Map<K, V2> remap(Map<K, V1> map,
                                               Function<? super V1, ? extends V2> function) {
        return map.entrySet()
                .stream() // or parallel
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> function.apply(e.getValue())
                ));
    }

}

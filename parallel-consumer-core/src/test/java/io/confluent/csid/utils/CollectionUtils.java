package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import java.util.List;
import java.util.Optional;

public class CollectionUtils {

    public static <T> Optional<T> getLast(List<T> history) {
        return history.isEmpty() ? Optional.empty() : Optional.of(history.get(history.size() - 1));
    }

}

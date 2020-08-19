package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;

import java.time.Duration;
import java.util.Optional;

public class BackportUtils {

    /**
     * @see Duration#toSeconds() intro'd in Java 9
     */
    static public long toSeconds(Duration duration) {
        return duration.toMillis() / 1000;
    }

    /**
     * @see Optional#isEmpty()  intro'd java 11
     */
    static public boolean isEmpty(Optional<?> optional){
        return !optional.isPresent();
    }
}

package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import org.slf4j.helpers.MessageFormatter;

public class StringUtils {

    public static String msg(String s, Object... args) {
        String message = MessageFormatter.basicArrayFormat(s, args);
        return message;
    }
}

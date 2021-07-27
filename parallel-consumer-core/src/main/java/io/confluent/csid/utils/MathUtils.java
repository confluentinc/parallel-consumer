package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

public class MathUtils {

    /**
     * @deprecated old use with AssertJ - not needed with Truth (has fuzzy matching built in)
     */
    public static boolean isLessWithin(final int needle, final int target, final int percent) {
        int i = target * (1 - percent / 100);

        int diff = target - needle;
        int norm = Math.abs(diff);
        int off = norm / target * 100;
        return off < percent;
    }

}

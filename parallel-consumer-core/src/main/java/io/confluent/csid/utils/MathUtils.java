package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

public class MathUtils {

    /**
     * @deprecated old, was used with AssertJ - not needed with Truth (has fuzzy matching built in)
     */
    @Deprecated
    public static boolean isLessWithin(final int needle, final int target, final int percent) {
        int i = target * (1 - percent / 100);

        int diff = target - needle;
        int norm = Math.abs(diff);
        int off = norm / target * 100;
        return off < percent;
    }

}

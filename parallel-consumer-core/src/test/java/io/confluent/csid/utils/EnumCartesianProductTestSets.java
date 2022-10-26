package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import org.junitpioneer.jupiter.CartesianProductTest;

/**
 * Automatically extract enum constants
 */
public class EnumCartesianProductTestSets extends CartesianProductTest.Sets {

    /**
     * Simply pass in the enum class, otherwise use as normal.
     *
     * @see CartesianProductTest.Sets#add
     */
    @Override
    public CartesianProductTest.Sets add(final Object... entries) {
        Object[] finalEntries = entries;
        if (entries.length == 1) {
            Object entry = entries[0];
            if (entry instanceof Class) {
                Class<?> classEntry = (Class<?>) entry;
                if (classEntry.isEnum()) {
                    finalEntries = classEntry.getEnumConstants();
                }
            }
        }
        return super.add(finalEntries);
    }

}

package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import org.junitpioneer.jupiter.cartesian.ArgumentSets;

/**
 * Automatically extract enum constants
 */
public class ArgumentSetsBuilder {
    ArgumentSets argumentSets;

    private ArgumentSetsBuilder() {
        argumentSets = ArgumentSets.create();
    }

    public static ArgumentSetsBuilder builder() {
        return new ArgumentSetsBuilder();
    }

    /**
     * Simply pass in the enum class, otherwise use as normal.
     *
     * @see ArgumentSets#argumentsForNextParameter
     */
    public ArgumentSetsBuilder add(final Object... entries) {
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
        argumentSets = argumentSets.argumentsForNextParameter(finalEntries);
        return this;
    }

    public ArgumentSets build() {
        return argumentSets;
    }

}

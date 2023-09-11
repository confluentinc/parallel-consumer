package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;
import org.assertj.core.presentation.StandardRepresentation;

@UtilityClass
public class StringTestUtils {

    public static final StandardRepresentation STANDARD_REPRESENTATION = new StandardRepresentation();

    public static String pretty(Object properties) {
        return STANDARD_REPRESENTATION.toStringOf(properties);
    }
}

package io.confluent.parallelconsumer.internal;

import lombok.experimental.UtilityClass;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
@UtilityClass
public class Documentation {

    public static final String DOCS_ROOT = "https://github.com/confluentinc/parallel-consumer/";

    public static String getLinkHtmlToDocSection(String section) {
        return DOCS_ROOT + section;
    }

}

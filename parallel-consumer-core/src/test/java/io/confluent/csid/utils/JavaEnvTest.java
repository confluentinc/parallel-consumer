package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static io.confluent.csid.utils.StringTestUtils.pretty;

@Slf4j
class JavaEnvTest {

    /**
     * Used to manually inspect the java environment at runtime - particularly useful for CI environments
     */
    @Test
    void checkJavaEnvironment() {
        log.error("Java all env: {}", pretty(System.getProperties().entrySet()));
    }
}

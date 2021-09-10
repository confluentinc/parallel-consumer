package io.confluent.csid.utils;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.presentation.StandardRepresentation;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.MessageFormatter;

import java.util.Map;
import java.util.Properties;

import static io.confluent.csid.utils.StringTestUtils.pretty;

@Slf4j
class JavaEnvTest {

    /**
     * Used to check the java environment at runtime
     */
    @Test
    void checkJavaEnvironment() {
        log.error("Java all env: {}", pretty(System.getProperties().entrySet()));
    }
}

package io.confluent.csid.utils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

/**
 * Print out test names in Maven output, not just class names. Useful for added context for failures in CI runs.
 *
 * Happy times... =D
 *
 * https://stackoverflow.com/questions/49937451/junit-5-is-it-possible-to-set-a-testexecutionlistener-in-maven-surefire-plugin
 * https://junit.org/junit5/docs/current/user-guide/#launcher-api-listeners-custom
 * https://github.com/google/auto/tree/master/service
 * https://stackoverflow.com/a/50058828/105741
 */
@AutoService(TestExecutionListener.class)
@Slf4j
public class TestLogger implements TestExecutionListener {
    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
//        log.info("-------------------------------------------------------");
//        log.info(testIdentifier.getDisplayName());
//        log.info("-------------------------------------------------------");
        System.out.println("-------------------------------------------------------");
        System.out.println(testIdentifier.getDisplayName());
        System.out.println("-------------------------------------------------------");
    }
}

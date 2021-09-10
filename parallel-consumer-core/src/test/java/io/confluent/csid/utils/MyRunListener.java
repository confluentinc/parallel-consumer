package io.confluent.csid.utils;

import com.google.auto.service.AutoService;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Print out test names in Maven output, not just class names. Useful for added context for failures in CI runs.
 * <p>
 * {@link AutoService} should work and load this automaticly, but something isn't working. So this is loaded manually
 * through the META-INF/serices/org.junit.platform.launcher.TestExecutionListener
 * <pre>
 *  https://stackoverflow.com/questions/49937451/junit-5-is-it-possible-to-set-a-testexecutionlistener-in-maven-surefire-plugin
 *  https://junit.org/junit5/docs/current/user-guide/#launcher-api-listeners-custom
 *  https://github.com/google/auto/tree/master/service
 *  https://stackoverflow.com/a/50058828/105741
 * </pre>
 */
@AutoService(TestExecutionListener.class)
public class MyRunListener implements TestExecutionListener {

    private final String template = """

            =========
               JUNIT {}:    {} ({})
            =========
            """;

    @Override
    public void testPlanExecutionStarted(final TestPlan testPlan) {
        log(msg(template, "Test plan execution started", testPlan, ""));
    }

    private void log(final String msg) {
        System.out.println(msg);
    }

    @Override
    public void executionSkipped(final TestIdentifier testIdentifier, final String reason) {
        log(msg(template, "skipped", testIdentifier.getDisplayName(), reason));
    }

    @Override
    public void executionStarted(final TestIdentifier testIdentifier) {
        log(msg(template, "started", testIdentifier.getDisplayName(), testIdentifier.getLegacyReportingName()));
    }

}

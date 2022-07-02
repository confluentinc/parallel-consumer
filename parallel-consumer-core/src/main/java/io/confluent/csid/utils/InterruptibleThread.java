package io.confluent.csid.utils;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Data
@Slf4j
public class InterruptibleThread {

    final Thread thread;

    @Getter
    static final ThreadLocal<Reason> objectThreadLocal = new ThreadLocal<>();

    Reason interruptReason = Reason.UNKNOWN;

    public static void logInterrupted(Logger delegateLogger, InterruptedException e) {
        logInterrupted(delegateLogger, "Interrupted", e);
    }

    public static void logInterrupted(Logger delegateLogger, Level level, InterruptedException e) {
        logInterrupted(delegateLogger, level, "No message given", e);
    }

    public static void logInterrupted(Logger delegateLogger, String msg, InterruptedException e) {
        logInterrupted(delegateLogger, Level.TRACE, msg + ": " + getInterruptReasonTL(), e);
    }

    public static void logInterrupted(Logger delegateLogger, Level level, String msg, InterruptedException e) {
        String msgCombined = msg + ": " + getInterruptReasonTL();
        // only Slf4j2 allows for dynamic levels - so use our own for now, until slf4j2 is widely adopted, if ever
        switch (level) {
            case ERROR -> delegateLogger.error(msgCombined, e);
            case WARN -> delegateLogger.warn(msgCombined, e);
            case INFO -> delegateLogger.info(msgCombined, e);
            case DEBUG -> delegateLogger.debug(msgCombined, e);
            case TRACE -> delegateLogger.trace(msgCombined, e);
        }
    }

    private static Reason getInterruptReasonTL() {
        return objectThreadLocal.get();
    }

    public void interrupt(Logger delegateLogger, Reason interruptReason) {
        this.interruptReason = interruptReason;
        objectThreadLocal.set(interruptReason);
        delegateLogger.debug("Interrupting thread {} for: {}", thread.getName(), interruptReason);
        thread.interrupt();
    }

    public String getName() {
        return thread.getName();
    }

    @AllArgsConstructor
    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class Reason {
        public static final Reason UNKNOWN = new Reason("unknown-reason");
        private final String desc;
        private Reason root;

//        public Reason(String msg, Reason reason) {
//            this()
//        }
    }
}

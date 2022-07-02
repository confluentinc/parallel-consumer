package io.confluent.csid.utils;

import lombok.Data;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
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

    public static void logInterrupted(InterruptedException e) {
        logInterrupted("Interrupted", e);
    }

    public static void logInterrupted(Level level, InterruptedException e) {
        logInterrupted(level, "No message given", e);
    }

    public static void logInterrupted(String msg, InterruptedException e) {
        logInterrupted(Level.TRACE, msg + ": " + getInterruptReasonTL(), e);
    }

    public static void logInterrupted(Level level, String msg, InterruptedException e) {
        log.atLevel(level).setCause(e).log(msg + ": " + getInterruptReasonTL());
    }

    private static Reason getInterruptReasonTL() {
        return objectThreadLocal.get();
    }


    public void interrupt(Reason interruptReason) {
        this.interruptReason = interruptReason;
        objectThreadLocal.set(interruptReason);
        thread.interrupt();
    }

    public String getName() {
        return thread.getName();
    }

    @Value
    public static class Reason {
        public static final Reason UNKNOWN = new Reason("unknown-reason");
        String desc;
    }
}

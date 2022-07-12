package io.confluent.csid.utils;

import io.confluent.csid.actors.Interruptible;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * todo docs, order class elements
 *
 * @author Antony Stubbs
 */
@Data
@Slf4j
// todo extract the logger, delete the rest - leave on a different branch?
public class InterruptibleThread {

    final Thread thread;

//    @Getter
//    static final ThreadLocal<Reason> objectThreadLocal = new ThreadLocal<>();
//    static final ThreadLocal<Reason> objectThreadLocal = new ThreadLocal<>();
//

    // todo make not static?
    private static Interruptible.Reason interruptReason = Interruptible.Reason.UNKNOWN;


    private static Interruptible.Reason getInterruptReasonTL() {
        var reason = interruptReason;
        clearReason();
        return reason;
    }

    private static void clearReason() {
        interruptReason = Interruptible.Reason.UNKNOWN;
    }

    public void interrupt(Logger delegateLogger, Interruptible.Reason interruptReason) {
        InterruptibleThread.interruptReason = interruptReason;
//        objectThreadLocal.set(interruptReason);
        delegateLogger.debug("Interrupting thread {} for: {}", thread.getName(), interruptReason);
        thread.interrupt();
    }

    public String getName() {
        return thread.getName();
    }

}

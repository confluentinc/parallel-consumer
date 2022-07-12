package io.confluent.csid.utils;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class InterruptLogger {
//    private static void logInterrupted(Logger delegateLogger, Level level, InterruptedException e) {
//        logInterrupted(delegateLogger, level, "No message given", e);
//    }
//
//    protected static void logInterrupted(Logger delegateLogger, InterruptedException e) {
//        logInterrupted(delegateLogger, "Interrupted", e);
//    }
//
//    private static void logInterrupted(Logger delegateLogger, String msg, InterruptedException e) {
//        logInterrupted(delegateLogger, Level.TRACE, msg + ": " + getInterruptReasonTL(), e);
//    }
//
//    private static void logInterrupted(Logger delegateLogger, Level level, String msg, InterruptedException e) {
//        //
//        Optional<String> locationString = getLocationString();
//        Interruptible.Reason reason = getInterruptReasonTL();
//
//        //
//        var reasonNormalised = reason == null ? "No reason given" : "Reason: " + reason;
//        var msgNormalised = msg == null ? "" : (msg + ". ");
//        var msgCombined = msg("{}{}. {}", msgNormalised, reasonNormalised, locationString.orElse("Can't find location"));
//
//        // only Slf4j2 allows for dynamic levels - so use our own for now, until slf4j2 is widely adopted, if ever
//        switch (level) {
//            case ERROR -> delegateLogger.error(msgCombined, e);
//            case WARN -> delegateLogger.warn(msgCombined, e);
//            case INFO -> delegateLogger.info(msgCombined, e);
//            case DEBUG -> delegateLogger.debug(msgCombined, e);
//            case TRACE -> delegateLogger.trace(msgCombined, e);
//        }
//
//        //
//        // out of scope for branch change - significant impact - move to experimental branch
////        Thread.currentThread().interrupt();
//    }
//
//    private static Optional<String> getLocationString() {
//        Optional<StackTraceElement> callerData = getCallerData();
//        return callerData.map(stackTraceElement -> {
//            String fileName = stackTraceElement.getFileName();
//            int lineNumber = stackTraceElement.getLineNumber();
//            String methodName = stackTraceElement.getMethodName();
//            return msg("({}:{}#{})", fileName, lineNumber, methodName);
//        });
//    }
//
//    private static Optional<StackTraceElement> getCallerData() {
//        Throwable cd = new Throwable();
//        StackTraceElement[] stackTrace = cd.getStackTrace();
//        Optional<StackTraceElement> caller = Optional.empty();
//        Class<InterruptibleThread> clazz = InterruptibleThread.class;
//        for (StackTraceElement stackTraceElement : stackTrace) {
//            String className = stackTraceElement.getClassName();
//            boolean contains = className.contains(clazz.getCanonicalName());
//            if (!contains) {
//                caller = of(stackTraceElement);
//                break;
//            }
//        }
//        return caller;
//    }
}

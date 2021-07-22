package io.confluent.csid.utils;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;

public class GeneralUtils {

    public static void checkLogging() {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        if (iLoggerFactory instanceof NOPLoggerFactory) {
            System.err.println("\n---\n" +
                    "Parallel Consumer: No SLF4J implementation detected - you will not get any log messages, including " +
                    "notification of issues processing messages. Please note the SLF4J errors printed above, and add a " +
                    "logging implementation." +
                    "\n---\n");
        }
    }

}

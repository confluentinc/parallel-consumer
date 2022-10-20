package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ExceptionInUserFunctionException;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.UserFunctions.Processor;
import lombok.experimental.UtilityClass;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Single entry point for wrapping the actual execution of user functions
 *
 * @author Antony Stubbs
 */
@UtilityClass
public class UserFunctions {

    public static final String MSG = "Error occurred in code supplied by user";

    /**
     * @param <PARAM_ONE>      the first in type for the user function
     * @param <PARAM_TWO>      the second in type for the user function
     * @param <RESULT>         the out type for the user function
     * @param wrappedFunction  the function to run
     * @param userFuncParamOne a parameter to pass into the user's function
     * @param userFuncParamTwo a parameter to pass into the user's function
     */
    public static <PARAM_ONE, PARAM_TWO, RESULT> RESULT carefullyRun(BiFunction<PARAM_ONE, PARAM_TWO, RESULT> wrappedFunction,
                                                                     PARAM_ONE userFuncParamOne,
                                                                     PARAM_TWO userFuncParamTwo) {
        try {
            return wrappedFunction.apply(userFuncParamOne, userFuncParamTwo);
        } catch (Throwable e) {
            throw new ExceptionInUserFunctionException(MSG, e);
        }
    }

    /**
     * @param <PARAM>         the in type for the user function
     * @param <RESULT>        the out type for the user function
     * @param wrappedFunction the function to run
     * @param userFuncParam   the parameter to pass into the user's function
     */
    public static <PARAM, RESULT> RESULT carefullyRun(Function<K, V> wrappedFunction, PollContext<K, V> userFuncParam) {
        try {
            return wrappedFunction.apply(userFuncParam);
        } catch (Throwable e) {
            throw new ExceptionInUserFunctionException(MSG, e);
        }
    }

    /**
     * @param <PARAM>         the in type for the user function
     * @param wrappedFunction the function to run
     * @param userFuncParam   the parameter to pass into the user's function
     */
    public static <K, V> void carefullyRun(Processor<K, V> wrappedFunction, PollContext<K, V> userFuncParam) {
        try {
            wrappedFunction.process(userFuncParam);
        } catch (Throwable e) {
            throw new ExceptionInUserFunctionException(MSG, e);
        }
    }

}

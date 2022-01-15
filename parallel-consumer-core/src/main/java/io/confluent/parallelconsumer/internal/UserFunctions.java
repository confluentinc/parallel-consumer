package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ErrorInUserFunctionException;
import lombok.experimental.UtilityClass;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single entry point for wrapping the actual execution of user functions
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
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

    /**
     * @param <PARAM>         the in type for the user function
     * @param <RESULT>        the out type for the user function
     * @param wrappedFunction the function to run
     * @param userFuncParam   the parameter to pass into the user's function
     */
    public static <PARAM, RESULT> RESULT carefullyRun(Function<PARAM, RESULT> wrappedFunction, PARAM userFuncParam) {
        try {
            return wrappedFunction.apply(userFuncParam);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

    /**
     * @param <PARAM>         the in type for the user function
     * @param wrappedFunction the function to run
     * @param userFuncParam   the parameter to pass into the user's function
     */
    public static <PARAM> void carefullyRun(Consumer<PARAM> wrappedFunction, PARAM userFuncParam) {
        try {
            wrappedFunction.accept(userFuncParam);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

}

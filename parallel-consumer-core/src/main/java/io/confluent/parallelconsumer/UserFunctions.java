package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@UtilityClass
public class UserFunctions {

    public static final String MSG = "Error occurred in code supplied by user";

    public static <T, U, R> R carefullyRun(BiFunction<T, U, R> wrappedFunction, T t, U u) {
        try {
            return wrappedFunction.apply(t, u);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

    public static <A, B> B carefullyRun(Function<A, B> wrappedFunction, A a) {
        try {
            return wrappedFunction.apply(a);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

    public static <A> void carefullyRun(Consumer<A> wrappedFunction, A a) {
        try {
            wrappedFunction.accept(a);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException(MSG, e);
        }
    }

}

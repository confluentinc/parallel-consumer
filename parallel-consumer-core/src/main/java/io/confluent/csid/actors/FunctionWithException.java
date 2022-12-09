package io.confluent.csid.actors;

/**
 * A copy of Java's {@link java.util.function.Function}, but which can throw exceptions.
 *
 * @author Antony Stubbs
 * @see java.util.function.Function
 */
@FunctionalInterface
public interface FunctionWithException<T, R> {

    /**
     * @see java.util.function.Function#apply
     */
    R apply(T t) throws Exception;

}

package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * <h1>Micro Actor framework for structured, ordered, simple IPC</h1>
 * <p>
 * By sending messages as Java Lambda (closure) functions, the Actor paradigm messages are effectively generated for us
 * automatically by the compiler. This takes the hassle out of creating message classes for every type of message we
 * want to send an actor - by effectively defining the message fields from the variables of the scope being closed over
 * and captured.
 * <h2>Sending</h2>
 * <p>
 * {@link ActorImpl} works in two parts - sending messages and processing them. Messages can be sent in two ways, by
 * {@link #ask}ing, or {@link #tell}ing.
 * <h3>Telling</h3>
 * <p>
 * The closure message is sent into the queue, with no response.
 * <h3>Asking</h3>
 * <p>
 * The closure message is sent into the queue, and a {@link Future} returned which will contain the response once it's
 * processed.
 * <p>
 * Messages can also be sent to the front of the queue, by using the {@code Immediately} versions of the methods. Note:
 * messages sent with these versions end up effectively in a "First In, Last Out" (FILO) queue, as a subsequent
 * immediate message will be in front of previous ones.
 * <h2>Processing</h2>
 * <p>
 * To process the closures, you must call one of the processing methods, and execution will be done in the calling
 * thread.
 * <p>
 * The {@link #process()} function iterates over all the closures in the queue, executing them serially.
 * <p>
 * The {@link #processBlocking(Duration)} version does the same initially, but then also will block by
 * {@link BlockingQueue#poll(long, TimeUnit)}ing the queue for new messages for the given duration, in order to allow
 * your program's thread to effectively {@link Thread#sleep} until new messages are sent.
 * <h2>Naming</h2>
 * Naming suggestions for classes declaring APIs which use the {@link ActorImpl}:
 * <p>
 * {@code Sync} denotes methods which will block using the {@link #ask} functions, waiting for a response.
 * <p>
 * {@code Async} denotes methods which will return immediately, after queueing a message.
 * <p>
 *
 * @param <T>
 * @author Antony Stubbs
 * @see ActorImpl
 */
public interface Actor<T> extends Interruptible {

    /**
     * Exceptions in execution will be logged
     */
    void tell(Consumer<T> action);

    /**
     * Same as {@link Actor#tell}, but messages will be placed at the front of the queue, instead of at the end.
     *
     * @see #tell
     */
    void tellImmediately(Consumer<T> action);

    /**
     * Same as {@link Actor#tellImmediately}, but returns a {@link Future} which will complete when the {@link Consumer}
     * has finished running. Including a completing exceptionally if there's an error running the {@link Consumer}.
     *
     * @see #tellImmediately
     */
    Future<Class<Void>> tellImmediatelyWithAck(Consumer<T> action);

    /**
     * Same as {@link Actor#tell} but messages will be placed at the front of the queue, instead of at the end.
     */
    <R> Future<R> ask(FunctionWithException<T, R> action);

    /**
     * Same as {@link Actor#ask}, but messages will be placed at the front of the queue, instead of at the end.
     *
     * @param <R> the type of the result
     * @return a {@link Future} which will contain the function result, once the closure has been processed by one of
     *         the {@link #process} methods.
     */
    <R> Future<R> askImmediately(FunctionWithException<T, R> action);

    /**
     * @return true if the queue is empty
     */
    boolean isEmpty();

    /**
     * Blocking version of {@link #process()}, will process messages, then block until either a new message arrives, or
     * the timeout is reached.
     */
    void processBlocking(Duration timeout) throws InterruptedException;

    /**
     * @return the number of actions in the queue
     */
    int getSize();

    /**
     * Processes the closures in the queued, in bounded element space.
     * <p>
     * Given the elements currently in the queue at the beginning of the method, processes them, but no more.
     * <p>
     * In other words - processes all elements currently in the queue, but not new ones which are added during
     * processing (the execution of this method). We do this so that we finish predictably and have no chance of taking
     * forever (which could happen if messages were continuously added between polling the queue for new messages).
     */
    void process();

    /**
     * The identifier of the actor
     */
    String getActorName();

    /**
     * Stop accepting any further messages, and then process any messages in the queue.
     */
    void close();

    /**
     * Start accepting messages.
     * <p>
     * Any messages sent before this will be rejected, as there's a chance they may never be processed.
     */
    // todo shouldn't this be triggerd through first process call?
    void start();

}

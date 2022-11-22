package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Micro Actor framework for structured, ordered, simple IPC.
 * <p>
 * By sending messages as Java Lambda (closure) functions, the Actor paradigm messages are effectively generated for us
 * automatically by the compiler. This takes the hassle out of creating message classes for every type of message we
 * want to send an actor - by effectively defining the message fields from the variables of the scope being closed over
 * and captured.
 * <p>
 * {@link Actor} works in two parts - sending messages and processing them. Messages can be sent in two ways, by
 * {@link #ask}ing, or {@link #tell}ing.
 * <p>
 * Telling
 * <p>
 * The closure message is sent into the queue, with no response.
 * <p>
 * Asking
 * <p>
 * The closure message is sent into the queue, and a {@link Future} returned which will contain the response once it's
 * processed.
 * <p>
 * Messages can also be sent to the front of the queue, by using the {@code Immediately} versions of the methods. Note:
 * messages sent with these versions end up effectively in a "First In, Last Out" (FILO) queue, as a subsequent
 * immediate message will be in front of previous ones.
 * <p>
 * Processing
 * <p>
 * To process the closures, you must call one of the processing methods, and execution will be done in the calling
 * thread.
 * <p>
 * The {@link #process()} function iterates over all the closures in the queue, executing them serially.
 * <p>
 * The {@link #processBlocking(Duration)} version does the same initially, but then also will block by
 * {@link BlockingQueue#poll(long, TimeUnit)}ing the queue for new messages for the given duration, in order to allow
 * your program's thread to effectively {@link Thread#sleep} until new messages are sent.
 * <p>
 * Naming suggestions for classes declaring APIs which use the {@link Actor}:
 * <p>
 * {@code Sync} denotes methods which will block using the {@link #ask} functions, waiting for a response.
 * <p>
 * todo drop?: , OR not run on the actor queue, but are Thread-Safe to call.
 * <p>
 * {@code Async} denotes methods which will return immediately, after queueing a message.
 * <p>
 *
 * @param <T> the Type of the Object to send closures to
 * @author Antony Stubbs
 */
@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
// todo rename to ActorRef? ActorInterface? ActorAPI? Also clashes with field name.
public class Actor<T> implements IActor<T>, Interruptible {

    private final T actorRef;

    private volatile ActorState state = ActorState.ACCEPTING_MESSAGES;

    /**
     * Single queueing point for all messages to the actor.
     * <p>
     * {@link LinkedBlockingDeque} is implemented as a simple doubly-linked list protected by a single lock and using
     * conditions to manage blocking. Thread safe, highly performant, single lock.
     */
    @Getter(AccessLevel.PROTECTED)
    private final LinkedBlockingDeque<Runnable> actionMailbox = new LinkedBlockingDeque<>();

    @Override
    public void tell(final Consumer<T> action) {
        checkState(ActorState.ACCEPTING_MESSAGES);
        getActionMailbox().add(() -> action.accept(actorRef));
    }

    @Override
    public void tellImmediately(final Consumer<T> action) {
        checkState(ActorState.ACCEPTING_MESSAGES);
        getActionMailbox().addFirst(() -> action.accept(actorRef));
    }

    @Override
    public <R> Future<R> ask(final Function<T, R> action) {
        checkState(ActorState.ACCEPTING_MESSAGES);

        /*
         * Consider using {@link CompletableFuture} instead - however {@link FutureTask} is just fine for PC.
         */
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actorRef));

        // queue
        getActionMailbox().add(task);

        return task;
    }

    private void checkState(ActorState targetState) {
        if (!state.equals(targetState)) {
            throw new InternalRuntimeException(msg("Actor in {} state, not {} target state", state, targetState));
        }
    }

    /**
     * Send a message to the actor, returning a Future with the result of the message.
     *
     * @param <R> the type of the result
     * @return a {@link Future} which will contain the function result, once the closure has been processed by one of
     *         the {@link #process} methods.
     */
    @Override
    public <R> Future<R> askImmediately(final Function<T, R> action) {
        checkState(ActorState.ACCEPTING_MESSAGES);

        FutureTask<R> task = new FutureTask<>(() -> action.apply(actorRef));
        getActionMailbox().addFirst(task);
        return task;
    }

    /**
     * @return true if the queue is empty
     */
    // todo only used from one place which is deprecated
    @Override
    public boolean isEmpty() {
        return this.getActionMailbox().isEmpty();
    }

    /**
     * @return the number of actions in the queue
     */
    public int getSize() {
        return this.getActionMailbox().size();
    }

    /**
     * Processes the closures in the queued, in bounded element space.
     * <p>
     * Given the elements currently in the queue at the beginning of the method, processes them, but no more.
     * <p>
     * In other words - processes all elements currently in the queue, but not new ones which are added during
     * processing (the execution of this method). We do this so that we finish predictably and have no chance of taking
     * forever (which could happen if messages were continuously added between polling the queue for new messages).
     * <p>
     * Does not execute scheduled - todo remove scheduled to subclass?
     */
    // todo in interface?
    @Override
    public void process() {
        BlockingQueue<Runnable> mailbox = this.getActionMailbox();

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = mailbox.size();
        log.trace("Processing mailbox - draining {}...", size);
        Deque<Runnable> work = new ArrayDeque<>(size);
        mailbox.drainTo(work, size);

        log.trace("Running {} drained actions...", work.size());
        for (var action : work) {
            execute(action);
        }
    }

    /**
     * Blocking version of {@link #process()}, will process messages, then block until either a new message arrives, or
     * the timeout is reached.
     */
    @Override
    public void processBlocking(Duration timeout) throws InterruptedException {
        process();
        maybeBlockUntilAction(timeout);
    }

    /**
     * May return without executing any scheduled actions
     *
     * @param timeout time to block for if mailbox is empty
     */
    private void maybeBlockUntilAction(Duration timeout) throws InterruptedException {
        if (!timeout.isNegative() && getActionMailbox().isEmpty()) {
            log.debug("Actor mailbox empty, polling with timeout of {}", timeout);
        }
        Runnable polled = getActionMailbox().poll(timeout.toMillis(), MILLISECONDS);

        if (polled != null) {
            log.debug("Message received in mailbox, processing");
            execute(polled);
            process();
        }
    }

    private void execute(@NonNull final Runnable command) {
        command.run();
    }

    /**
     * A simple convenience method to push an effectively NO-OP message to the actor, which would wake it up if it were
     * blocked polling the queue for a new message. Useful to have a blocked thread return from the process method if
     * it's blocked, without needed to {@link Thread#interrupt} it, but you don't want to send it a closure for some
     * reason.
     *
     * @param reason the reason for interrupting the Actor
     * @deprecated rather than call this generic wakeup method, it's better to send a message directly to your Actor
     *         object {@link T} (todo how to link to type param), so that the interrupt has context. However this can be
     *         useful to use for legacy code.
     */
    @Deprecated
    @Override
    public void interruptMaybePollingActor(Reason reason) {
        log.debug(msg("Adding interrupt signal to queue of {}: {}", getActorName(), reason));
        getActionMailbox().add(() -> interruptInternalSync(reason));
    }

    @Override
    public String getActorName() {
        return actorRef.getClass().getSimpleName();
    }

    /**
     * Stop accepting any further messages, and then process any closures in the queue.
     */
    @Override
    public void close() {
        state = ActorState.CLOSED;
        process();
    }

    /**
     * Perform the NO-OP interrupt.
     * <p>
     * Note: Might not have actually interrupted a sleeping {@link BlockingQueue#poll()} if there was also other work on
     * the queue.
     */
    private void interruptInternalSync(Reason reason) {
        log.debug("Interruption signal processed: {}", reason);
    }

    public enum ActorState {
        ACCEPTING_MESSAGES,
        CLOSED
    }
}

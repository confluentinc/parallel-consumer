package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import io.confluent.parallelconsumer.internal.ThreadSafe;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.confluent.csid.actors.ActorImpl.ActorState.CLOSED;
import static io.confluent.csid.utils.StringUtils.msg;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Antony Stubbs
 * @see Actor
 */
@Slf4j
@ToString
@ThreadSafe
@EqualsAndHashCode
@RequiredArgsConstructor
public class ActorImpl<T> implements Actor<T> {

    /**
     * The direct reference to the instance to act upon.
     */
    private final T actorRef;

    /**
     * Current state of the Actor.
     */
    private final AtomicReference<ActorState> state = new AtomicReference<>(ActorState.NOT_STARTED);

    /**
     * Single queueing point for all messages to the actor.
     * <p>
     * {@link LinkedBlockingDeque} is implemented as a simple doubly-linked list protected by a single lock and using
     * conditions to manage blocking. Thread safe, highly performant, single lock.
     */
    @Getter(AccessLevel.PRIVATE)
    private final LinkedBlockingDeque<Runnable> actionMailbox = new LinkedBlockingDeque<>();

    /**
     * Guards state transitions
     */
    private final ReentrantLock stateLock = new ReentrantLock();

    /**
     * For notifying waiting threads that our state has changed
     */
    private final Condition stateChanged = stateLock.newCondition();

    /**
     * Timeout for state change waiting
     */
    private final Duration stateTimeout = Duration.ofSeconds(10);

    /**
     * If true, blocks calls to the actor until it is accepting messages
     */
    boolean waitForState = true;

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
    public Future<Class<Void>> tellImmediatelyWithAck(Consumer<T> action) {
        FunctionWithException<T, Class<Void>> funcWrap = actor -> {
            action.accept(actor);
            return Void.TYPE;
        };
        return askImmediately(funcWrap);
    }

    @Override
    public <R> Future<R> ask(FunctionWithException<T, R> action) {
        CompletableFuture<R> future = checkStateFuture(ActorState.ACCEPTING_MESSAGES);
        Runnable runnable = createRunnable(action, future);
        getActionMailbox().add(runnable);
        return future;
    }

    private <R> CompletableFuture<R> checkStateFuture(ActorState targetState) {
        return waitForState
                ? waitForState(targetState)
                : errorIfNotState(targetState);
    }

    private <R> CompletableFuture<R> errorIfNotState(ActorState targetState) {
        if (targetState.equals(state.get())) {
            return new CompletableFuture<>();
        }

        var message = state.get() == ActorState.NOT_STARTED
                ? "Actor for class {} is not started yet ({}) - call `#start` first"
                : "Actor for class {} in {} state, not {} target state";
        var result = new InternalRuntimeException(msg(
                message,
                actorRef.getClass().getSimpleName(),
                state.get(),
                targetState));

        // CompletableFuture.failedFuture(result); @since 1.9
        var future = new CompletableFuture<R>();
        future.completeExceptionally(result);
        return future;
    }

    /**
     * Return a completable future that will complete when the state is reached,
     * <p>
     * Or fail if the state is not reached within the timeout.
     */
    private <R> CompletableFuture<R> waitForState(ActorState targetState) {
        if (CLOSED.equals(state.get())) {
            return errorIfNotState(targetState);
        }

        var future = new CompletableFuture<R>();
        log.debug("Waiting for state {} to be reached", targetState);
        var timer = Time.SYSTEM.timer(stateTimeout);
        while (!state.get().equals(targetState)) {
            stateLock.lock();
            try {
                stateChanged.await(stateTimeout.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
                return future;
            } finally {
                stateLock.unlock();
            }

            if (timer.isExpired()) {
                future.completeExceptionally(new InternalRuntimeException(msg("Timed out waiting for state {} to be reached", targetState)));
                return future;
            }
        }
        log.debug("State {} reached", state.get());
        return future;
    }

    private <R> Runnable createRunnable(FunctionWithException<T, R> action, CompletableFuture<R> future) {
        return () -> {
            try {
                var apply = action.apply(actorRef);
                future.complete(apply);
            } catch (Exception e) {
                log.error("Error in actor task", e);
                future.completeExceptionally(e);
            }
        };
    }

    @Override
    public <R> Future<R> askImmediately(FunctionWithException<T, R> action) {
        CompletableFuture<R> future = checkStateFuture(ActorState.ACCEPTING_MESSAGES);
        var task = createRunnable(action, future);
        getActionMailbox().addFirst(task);
        return future;
    }

    // todo only used from one place which is deprecated
    @Override
    public boolean isEmpty() {
        return this.getActionMailbox().isEmpty();
    }

    @Override
    public void processBlocking(Duration timeout) throws InterruptedException {
        var processed = process();

        if (processed <= 0) {
            // wait for a message to arrive
            maybeBlockUntilAction(timeout);
        }
    }

    @Override
    public int getSize() {
        return this.getActionMailbox().size();
    }

    @Override
    public int process() {
        start();
        return processWithoutStarting();
    }

    private int processWithoutStarting() {
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

        return work.size();
    }

    @Override
    public String getActorName() {
        return actorRef.getClass().getSimpleName();
    }

    @Override
    public void close() {
        transitionStateTo(CLOSED);
        processWithoutStarting();
    }

    private void transitionStateTo(ActorState newState) {
        stateLock.lock();
        try {
            state.set(newState);
            stateChanged.signalAll();
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void start() {
        transitionStateTo(ActorState.ACCEPTING_MESSAGES);
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
        Runnable triggerPoll = getActionMailbox().poll(timeout.toMillis(), MILLISECONDS);

        if (triggerPoll != null) {
            log.debug("Trigger message received in mailbox, processing");
            execute(triggerPoll);
            // process remaining messages, if any
            process();
        }
    }

    private void execute(@NonNull final Runnable command) {
        command.run();
    }

    @SneakyThrows
    private void checkState(ActorState targetState) {
        var ready = waitForState
                ? waitForState(targetState)
                : errorIfNotState(targetState);

        // get state in case it was an error - enables us to reuse the same functions for ask and tell
        if (ready.isCompletedExceptionally()) {
            ready.get();
        }
    }

    @Override
    public void interrupt(Reason reason) {
        log.debug(msg("Adding interrupt signal to queue of {}: {}", getActorName(), reason));
        getActionMailbox().add(() -> interruptInternal(reason));
    }

    /**
     * Perform the NO-OP interrupt.
     * <p>
     * Note: Might not have actually interrupted a sleeping {@link BlockingQueue#poll()} if there was also other work on
     * the queue.
     */
    private void interruptInternal(Reason reason) {
        log.debug("Interruption signal processed: {}", reason);
    }

    // todo TG needs this public - fix - should not be accessing - make private
    public enum ActorState {
        NOT_STARTED,
        ACCEPTING_MESSAGES,
        CLOSED
    }
}

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Antony Stubbs
 * @see IActorAPI
 */
@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class Actor<T> implements IActorAPI<T>, Interruptible {

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
    public <R> Future<R> ask(FunctionWithException<T, R> action) {
        CompletableFuture<R> future = checkStateFuture(ActorState.ACCEPTING_MESSAGES);
        Runnable runnable = createRunnable(action, future);
        getActionMailbox().add(runnable);
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

    private void checkState(ActorState targetState) {
        if (!targetState.equals(state.get())) {
            throw new InternalRuntimeException(msg("Actor in {} state, not {} target state", state.get(), targetState));
        }
    }

    private <R> CompletableFuture<R> checkStateFuture(ActorState targetState) {
        if (targetState.equals(state.get())) {
            return new CompletableFuture<>();
        }

        var message = state.get() == ActorState.NOT_STARTED
                ? "Actor is not started yet ({}) - call `#start` first"
                : "Actor in {} state, not {} target state";
        var result = new InternalRuntimeException(msg(
                message,
                state.get(),
                targetState));

        // CompletableFuture.failedFuture(result); @since 1.9
        var future = new CompletableFuture<R>();
        future.completeExceptionally(result);
        return future;
    }

    @Override
    public <R> Future<R> askImmediately(FunctionWithException<T, R> action) {
        CompletableFuture<R> future = checkStateFuture(ActorState.ACCEPTING_MESSAGES);
        var task = createRunnable(action, future);
        getActionMailbox().addFirst(task);
        return future;
    }

    public Future<Class<Void>> askImmediatelyVoid(Consumer<T> action) {
        FunctionWithException<T, Class<Void>> funcWrap = actor -> {
            action.accept(actor);
            return Void.TYPE;
        };
        return askImmediately(funcWrap);
    }

    // todo only used from one place which is deprecated
    @Override
    public boolean isEmpty() {
        return this.getActionMailbox().isEmpty();
    }

    @Override
    public int getSize() {
        return this.getActionMailbox().size();
    }

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

    @Override
    public void interruptMaybePollingActor(Reason reason) {
        log.debug(msg("Adding interrupt signal to queue of {}: {}", getActorName(), reason));
        getActionMailbox().add(() -> interruptInternalSync(reason));
    }

    @Override
    public String getActorName() {
        return actorRef.getClass().getSimpleName();
    }

    @Override
    public void close() {
        state.set(ActorState.CLOSED);
        process();
    }

    @Override
    public void start() {
        state.set(ActorState.ACCEPTING_MESSAGES);
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
        NOT_STARTED,
        ACCEPTING_MESSAGES,
        CLOSED
    }
}

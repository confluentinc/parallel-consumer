package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.InterruptibleThread;
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
 * todo docs
 *
 * @param <T>
 * @author Antony Stubbs
 */
@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
// rename to ActorRef? Also clashes with field name.
public class Actor<T> implements IActor<T>, Executor {

    private final T actor;

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
        getActionMailbox().add(() -> action.accept(actor));
    }

    @Override
    public void tellImmediately(final Consumer<T> action) {
        getActionMailbox().addFirst(() -> action.accept(actor));
    }

    @Override
    public <R> Future<R> ask(final Function<T, R> action) {
        /*
         * Consider using {@link CompletableFuture} instead - however {@link FutureTask} is just fine for PC.
         */
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actor));

        // todo should actor throw invalid state if actor is "closed" or "terminated"?
        getActionMailbox().add(task);

// how to use CompletableFuture instead?
//        CompletableFuture<R> future = new CompletableFuture<>();
//        future.handleAsync()
//        future.newIncompleteFuture();

        return task;
    }

    @Override
    public <R> Future<R> askImmediately(final Function<T, R> action) {
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actor));
        getActionMailbox().addFirst(task);
        return task;
    }

    // todo only used from one place which is deprecated
    @Override
    public boolean isEmpty() {
        return this.getActionMailbox().isEmpty();
    }


    /**
     * Given the elements currently in the queue, processes them, but no more. In other words - processes all elements
     * currently in the queue, but not new ones which are added during processing. We do this so that we finish
     * predictably and have no chance of processing forever.
     * <p>
     * Does not execute scheduled - todo remove scheduled to subclass?
     */
    // todo in interface?
    public void processBounded() {
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
     * Blocking version of {@link #processBounded()}
     */
    // todo in interface?
    public void processBlocking(Duration timeout) throws InterruptedException {
        processBounded();
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
            processBounded();
        }
    }

    private void execute(@NonNull final Runnable command) {
        command.run();
    }

    public void interruptProcessBlockingMaybe(InterruptibleThread.Reason reason) {
        log.debug(msg("Adding interrupt signal to queue of {}: {}", getActorName(), reason));
        getActionMailbox().add(() -> interruptInternal(reason));
    }

    private String getActorName() {
        return actor.getClass().getSimpleName();
    }

    /**
     * Might not have actually interrupted a sleeping {@link BlockingQueue#poll()} if there was also other work on the
     * queue.
     */
    private void interruptInternal(InterruptibleThread.Reason reason) {
        log.debug("Interruption signal processed: {}", reason);
    }
}

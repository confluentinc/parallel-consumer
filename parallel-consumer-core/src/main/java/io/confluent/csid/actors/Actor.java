package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

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
 * automatically by the compiler. This takes the hassle out of creating messages classes for every type of message you
 * want to send an actor - by effectively defining the message content in the parameters of the method being called.
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
 * The closure message is sent into the queue, and a {@link Future} returned which will containing the response once
 * it's processed.
 * <p>
 * Processing
 * <p>
 * The {@link #process()} function iterates over all the closures in the queue, executing them serially. The
 * {@link #processBlocking(Duration)} version does the same initially, but then also will block by
 * {@link BlockingQueue#poll(long, TimeUnit)}ing the queue for new messages for the given duration, in order to allow
 * your program's thread to effectively {@link Thread#sleep} until new messages are sent.
 * <p>
 * Naming suggestions:
 * <p>
 * {@code Sync} denotes methods which will not run on the actor queue.
 * <p>
 * {@code Async} denotes methods which will return immediately, after queueing a message.
 * <p>
 * Messages can also be sent to the front of the queue, by using the {@code Immediately} versions of the methods.
 *
 * @param <T>
 * @author Antony Stubbs
 */
@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
// todo rename to ActorRef? ActorInterface? ActorAPI? Also clashes with field name.
public class Actor<T> implements IActor<T>, Interruptable {

    private final T actualActorInstance;

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
        getActionMailbox().add(() -> action.accept(actualActorInstance));
    }

    @Override
    public void tellImmediately(final Consumer<T> action) {
        getActionMailbox().addFirst(() -> action.accept(actualActorInstance));
    }

    @Override
    public <R> Future<R> ask(final Function<T, R> action) {
        /*
         * Consider using {@link CompletableFuture} instead - however {@link FutureTask} is just fine for PC.
         */
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actualActorInstance));

        // todo should actor throw invalid state if actor is "closed" or "terminated"?
        getActionMailbox().add(task);

        return task;
    }

    @Override
    public <R> Future<R> askImmediately(final Function<T, R> action) {
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actualActorInstance));
        getActionMailbox().addFirst(task);
        return task;
    }

    // todo only used from one place which is deprecated
    @Override
    public boolean isEmpty() {
        return this.getActionMailbox().isEmpty();
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
     * Blocking version of {@link #process()}
     */
    // todo in interface?
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
    public void interruptProcessAsync(Reason reason) {
        log.debug(msg("Adding interrupt signal to queue of {}: {}", getActorName(), reason));
        getActionMailbox().add(() -> interruptInternalSync(reason));
    }

    private String getActorName() {
        return actualActorInstance.getClass().getSimpleName();
    }

    /**
     * Might not have actually interrupted a sleeping {@link BlockingQueue#poll()} if there was also other work on the
     * queue.
     */
    private void interruptInternalSync(Reason reason) {
        log.debug("Interruption signal processed: {}", reason);
    }
}

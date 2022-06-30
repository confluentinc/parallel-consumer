package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @param <T>
 * @author Antony Stubbs
 */
@Slf4j
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
// todo rename just to actor? move to csid,
public class ActorRef<T> implements IActor<T>, Executor {

    private final T actor;

    //    /**
//     * Object because there's no common interface between {@link Function} and {@link Consumer}
//     */
    @Getter
//    private final BlockingQueue<Callable<Optional<Object>>> actionMailbox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non-blocking
    private final LinkedBlockingQueue<Runnable> actionMailbox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non-blocking

    @Getter
    private final PriorityQueue<Scheduled> scheduled = new PriorityQueue<>();

    @Override
    public void tell(final Consumer<T> action) {
        getActionMailbox().add(() -> action.accept(actor));

//        getActionMailbox().add(() -> {
//            action.accept(actor);
//            return Optional.empty();
//        });
    }

    @Override
    public <R> Future<R> ask(final Function<T, R> action) {
//        Callable<Optional<R>> task = () -> Optional.of(action.apply(actor));
        FutureTask<R> task = new FutureTask<>(() -> action.apply(actor));
//        getActionMailbox().add((Callable) task);
        getActionMailbox().add(task);

// how to use CompletableFuture instead?
//        CompletableFuture<R> future = new CompletableFuture<>();
//        future.handleAsync()
//        future.newIncompleteFuture();

        return task;
    }

    private final ScheduledExecutorService timer;

    public void later(Consumer<T> action, Duration when) {
        timer.schedule(() -> action.accept(actor), when.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Given the elements currently in the queue, processes them, but no more. In other words - processes all elements
     * currently in the queue, but not new ones which are added during processing. We do this so that we finish
     * predictably and have no chance of processing forever.
     */
    private void processBounded() {
        BlockingQueue<Runnable> mailbox = this.getActionMailbox();

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = mailbox.size();
        log.trace("Processing mailbox - draining {}...", size);
        Deque<Runnable> work = new ArrayDeque<>(size);
        mailbox.drainTo(work, size);

        log.trace("Running {} drained actions...", work.size());
        for (var action : work) {
//            action.run();
            execute(action);
        }
//
//        actionMailbox.forEach(
//                //                action.accept(this)
//                Runnable::run
//        );
    }

    /**
     * Blocking version of {@link #processBounded()}
     *
     * @param timeout
     */
    public void processBlocking(Duration timeout) {
        processBounded();
        maybeBlockUntilScheduledOrAction(timeout);
    }

    /**
     * May return without executing any scheduled actions
     *
     * @param timeout
     */
    private void maybeBlockUntilScheduledOrAction(final Duration timeout) {
        Duration timeToBlockFor = lowerOfScheduledOrTimeout(timeout);
        var interrupted = getActionMailbox().poll(timeToBlockFor.toMillis(), MILLISECONDS);

        if (interrupted != null) {
            execute(interrupted);
            processBounded();
        } else {
            maybeExecuteScheduled();
        }
    }

    private final Clock time;

    private Duration lowerOfScheduledOrTimeout(Duration timeout) {
        Scheduled task = getScheduled().peek();
        Instant now = time.instant();
        boolean taskIsScheduledBeforeTimeout = task.getWhen().isBefore(now.plus(timeout));
        return taskIsScheduledBeforeTimeout ? Duration.between(now, task.getWhen()) : timeout;
    }

    private void maybeExecuteScheduled() {
        Scheduled task = getScheduled().peek();
        if (task.itsTime()) {
            execute(task.getWhat());
        }
    }

    @Override
    public void execute(@NonNull final Runnable command) {
        command.run();
    }

    @Value
    class Scheduled {
        Instant when;
        Runnable what;
        Clock time;

        public boolean itsTime() {
            return time.instant().isAfter(getWhen());
        }
    }
}

package io.confluent.parallelconsumer.internal;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.State.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PRIVATE;

/**
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = PRIVATE)
public class StateMachine implements DrainingCloseable {

    PCModule<?, ?> module;

    Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    /**
     * The run state of the controller.
     *
     * @see State
     */
    @Getter
    @NonFinal
    State state = State.unused;

    /**
     * If the system failed with an exception, it is referenced here.
     */
    @NonFinal
    private Exception failureReason;

    /**
     * Close the system, without draining.
     *
     * @see State#draining
     */
    @Override
    public void close() {
        // use a longer timeout, to cover for evey other step using the default
        Duration timeout = DrainingCloseable.DEFAULT_TIMEOUT.multipliedBy(2);
        closeDontDrainFirst(timeout);
    }

    @Override
    @SneakyThrows
    public void close(Duration timeout, DrainingCloseable.DrainingMode drainMode) {
        if (state == closed) {
            log.info("Already closed, checking end state..");
        } else {
            log.info("Signaling to close...");

            switch (drainMode) {
                case DRAIN -> {
                    log.info("Will wait for all in flight to complete before");
                    transitionToDraining();
                }
                case DONT_DRAIN -> {
                    log.info("Not waiting for remaining queued to complete, will finish in flight, then close...");
                    transitionToClosing();
                }
            }

            waitForClose(timeout);
        }

        if (controlThreadFuture.isPresent()) {
            log.debug("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(timeout.toMillis(), MILLISECONDS); // throws exception if supervisor saw one
        }

        log.info("Close complete.");
    }

    private void waitForClose(Duration timeout) throws TimeoutException, ExecutionException {
        log.info("Waiting on closed state...");
        while (!state.equals(closed)) {
            try {
                Future<Boolean> booleanFuture = this.controlThreadFuture.get();
                log.debug("Blocking on control future");
                boolean signaled = booleanFuture.get(toSeconds(timeout), SECONDS);
                if (!signaled)
                    throw new TimeoutException("Timeout waiting for system to close (" + timeout + ")");
            } catch (InterruptedException e) {
                // ignore
                log.trace("Interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                log.error("Execution or timeout exception while waiting for the control thread to close cleanly " +
                        "(state was {}). Try increasing your time-out to allow the system to drain, or close without " +
                        "draining.", state, e);
                throw e;
            }
            log.trace("Still waiting for system to close...");
        }
    }

    protected boolean isIdlingOrRunning() {
        return state == running || state == draining || state == paused;
    }

    // todo not used?
    //    @Override
    public void resumeIfPaused() {
        if (this.state == State.paused) {
            log.info("Transitioning parallel consumer to state running.");
            this.state = State.running;
            module.controller().notifySomethingToDo();
        } else {
            log.debug("Skipping transition of parallel consumer to state running. Current state is {}.", this.state);
        }
    }

    private void transitionToDraining() {
        log.debug("Transitioning to draining...");
        this.state = State.draining;
        module.controller().notifySomethingToDo();
    }

    protected void drain() {
        log.debug("Signaling to drain...");
        module.brokerPoller().drain();
        if (!isRecordsAwaitingProcessing()) {
            transitionToClosing();
        } else {
            log.debug("Records still waiting processing, won't transition to closing.");
        }
    }

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = module.workManager().isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    public boolean isOpen() {
        return state != closed;
    }

    private void transitionToClosing() {
        log.debug("Transitioning to closing...");
        if (state == State.unused) {
            state = closed;
        } else {
            state = State.closing;
        }
        module.controller().notifySomethingToDo();
    }

    public boolean isClosedOrFailed() {
        boolean closed = state == State.closed;
        boolean doneOrCancelled = false;
        if (this.controlThreadFuture.isPresent()) {
            Future<Boolean> threadFuture = controlThreadFuture.get();
            doneOrCancelled = threadFuture.isDone() || threadFuture.isCancelled();
        }
        return closed || doneOrCancelled;
    }


    //    @Override
    public void pauseIfRunning() {
        if (this.state == State.running) {
            log.info("Transitioning parallel consumer to state paused.");
            this.state = State.paused;
        } else {
            log.debug("Skipping transition of parallel consumer to state paused. Current state is {}.", this.state);
        }
    }

    protected void maybeTransitionState() throws TimeoutException, ExecutionException, InterruptedException {
        log.trace("Current state: {}", state);
        switch (state) {
            case draining -> {
                drain();
            }
            case closing -> {
                doClose(DrainingCloseable.DEFAULT_TIMEOUT);
            }
        }
    }

    /**
     * @return if the system failed, returns the recorded reason.
     */
    public Exception getFailureCause() {
        return this.failureReason;
    }

    public void transitionToRunning() {
        if (state == State.unused) {
            state = running;
        } else {
            throw new IllegalStateException(msg("Invalid state - you cannot call the poll* or pollAndProduce* methods " +
                    "more than once (they are asynchronous) (current state is {})", state));
        }
    }

    protected void doClose(Duration timeout) throws TimeoutException, ExecutionException, InterruptedException {
        log.debug("Starting close process (state: {})...", state);

        log.debug("Awaiting worker pool termination...");
        module.controlLoop().awaitControlLoopClose(timeout);

        // last check to see if after worker pool closed, has any new work arrived?
        module.workMailbox().processWorkCompleteMailBox(Duration.ZERO);

        //
        module.controlLoop().commitOffsetsThatAreReady();

        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        module.brokerPoller().closeAndWait();

        maybeCloseConsumer();

        module.producerManager().ifPresent(x -> x.close(timeout));

        log.debug("Close complete.");
        this.state = closed;

        if (this.getFailureCause() != null) {
            log.error(msg("PC closed due to error: {}", getFailureCause().getMessage()), getFailureCause());
        }
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumer() {
        if (isResponsibleForCommits()) {
            module.consumer().close();
        }
    }

    private boolean areMyThreadsDone() {
        if (isEmpty(controlThreadFuture)) {
            // not constructed yet, will become alive, unless #poll is never called
            return false;
        } else {
            return controlThreadFuture.get().isDone();
        }
    }

    protected Exception handleCrash(Exception e) throws TimeoutException, ExecutionException, InterruptedException {
        log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
        failureReason = new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
        doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
        return failureReason;
    }

    private boolean isResponsibleForCommits() {
        return (module.committer() instanceof ProducerManager);
    }


    public boolean isRunning() {
        return state == running;
    }

    @Override
    public long workRemaining() {
        return module.workManager().getNumberOfIncompleteOffsets();
    }
}
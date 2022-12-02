package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.lang.Nullable;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.parallelconsumer.internal.State.running;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * Main Control loop for the parallel consumer.
 *
 * @author Antony Stubbs
 */
@Slf4j
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class ControlLoop<K, V> {

    PCModule<K, V> module;

    /**
     * Useful for testing async code
     */
    List<Runnable> controlLoopHooks = new ArrayList<>();

    ParallelConsumerOptions<K, V> options;

    @Getter(PROTECTED)
    @NonFinal
    @Nullable
    PCWorkerPool<K, V, Object> workerPool;

    // todo make private
    @Getter(PRIVATE)
    WorkMailbox<K, V> workMailbox;

    BrokerPollSystem<?, ?> brokerPollSubsystem;

    @Getter(PRIVATE)
    WorkManager<K, V> wm;

    StateMachine state;

    // todo delete
//    RateLimiter queueStatsLimiter = new RateLimiter();

    // todo depends on MicroMeter pr
    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer workRetrievalTimer = metricsRegistry.timer("user.function");

    /**
     * Used to request a commit asap
     */
    AtomicBoolean commitCommand = new AtomicBoolean(false);

    /**
     * Time of last successful commit
     */
    @NonFinal
    Instant lastCommitTime;

    @NonFinal
    Instant lastCommitCheckTime = Instant.now();

    public ControlLoop(PCModule<K, V> module) {
        this.module = module;
        options = module.options();
        wm = module.workManager();
        brokerPollSubsystem = module.brokerPoller();
        state = module.stateMachine();
        workMailbox = module.workMailbox();

    }

    // todo make private
    protected void initWorkerPool(Function userFunctionWrapped, Consumer<Object> callback) {
        // todo casts
        Function<PollContextInternal<K, V>, List<Object>> cast = userFunctionWrapped;
        var runner = FunctionRunner.<K, V, Object>builder()
                .userFunctionWrapped(cast)
                .callback(callback)
                .options(options)
                .module(module)
                .workMailbox(workMailbox)
                .workManager(wm)
                .build();

        workerPool = new PCWorkerPool<>(options.getMaxConcurrency(), runner, options);
    }

    /**
     * Main control loop
     */
    protected void loop() throws TimeoutException, ExecutionException, InterruptedException {
        Objects.requireNonNull(workerPool);

        maybeWakeupPoller();

        //
        final boolean shouldTryCommitNow = maybeAcquireCommitLock();

        // make sure all work that's been completed are arranged ready for commit
        Duration timeToBlockFor = shouldTryCommitNow ? Duration.ZERO : getTimeToBlockFor();
        workMailbox.processWorkCompleteMailBox(timeToBlockFor);

        //
        if (shouldTryCommitNow) {
            // offsets will be committed when the consumer has its partitions revoked
            commitOffsetsThatAreReady();
        }

        // distribute more work
        retrieveAndDistributeNewWorkNew();

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        state.maybeTransitionState();

        // thread yield for spin lock avoidance
        Duration duration = Duration.ofMillis(1);
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            log.trace("Woke up", e);
        }
    }

    /**
     * If we don't have enough work queued, and the poller is paused for throttling,
     * <p>
     * todo move into {@link WorkManager} as it's specific to WM having enough work?
     */
    private void maybeWakeupPoller() {
        if (state.isRunning()) {
            if (!wm.isSufficientlyLoaded() && brokerPollSubsystem.isPausedForThrottling()) {
                log.debug("Found Poller paused with not enough front loaded messages, ensuring poller is awake (mail: {} vs target: {})",
                        wm.getNumberOfWorkQueuedInShardsAwaitingSelection(),
                        options.getTargetAmountOfRecordsInFlight());
                brokerPollSubsystem.wakeupIfPaused();
            }
        }
    }


    /**
     * If it's time to commit, and using transactional system, tries to acquire the commit lock.
     * <p>
     * Call {@link ProducerManager#preAcquireOffsetsToCommit()} early, to initiate the record sending barrier for this
     * transaction (so no more records can be sent, before collecting offsets to commit).
     *
     * @return true if committing should either way be attempted now
     */
    private boolean maybeAcquireCommitLock() throws TimeoutException, InterruptedException {
        final boolean shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty();
        // could do this optimistically as well, and only get the lock if it's time to commit, so is not frequent
        if (shouldTryCommitNow && options.isUsingTransactionCommitMode()) {
            // get into write lock queue, so that no new work can be started from here on
            log.debug("Acquiring commit lock pessimistically, before we try to collect offsets for committing");
            //noinspection OptionalGetWithoutIsPresent - options will already be verified
            module.producerManager().get().preAcquireOffsetsToCommit();
        }
        return shouldTryCommitNow;
    }

    protected boolean isTimeToCommitNow() {
        updateLastCommitCheckTime();

        Duration elapsedSinceLastCommit = this.lastCommitTime == null ? Duration.ofDays(1) : Duration.between(this.lastCommitTime, Instant.now());

        boolean commitFrequencyOK = elapsedSinceLastCommit.compareTo(getTimeBetweenCommits()) > 0;
        boolean lingerBeneficial = lingeringOnCommitWouldBeBeneficial();
        boolean isCommandedToCommit = isCommandedToCommit();

        boolean shouldDoANormalCommit = commitFrequencyOK && !lingerBeneficial;

        boolean shouldCommitNow = shouldDoANormalCommit || isCommandedToCommit;

        if (log.isDebugEnabled()) {
            log.debug("Should commit this cycle? " +
                    "shouldCommitNow? " + shouldCommitNow + " : " +
                    "shouldDoANormalCommit? " + shouldDoANormalCommit + ", " +
                    "commitFrequencyOK? " + commitFrequencyOK + ", " +
                    "lingerBeneficial? " + lingerBeneficial + ", " +
                    "isCommandedToCommit? " + isCommandedToCommit
            );
        }

        return shouldCommitNow;
    }

    /**
     * Request a commit as soon as possible (ASAP), overriding other constraints.
     */
    public void requestCommitAsap() {
        log.debug("Registering command to commit next chance");
        synchronized (commitCommand) {
            this.commitCommand.set(true);
        }
        module.controller().notifySomethingToDo();
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            return this.commitCommand.get();
        }
    }

    private void updateLastCommitCheckTime() {
        lastCommitCheckTime = Instant.now();
    }


    /**
     * Gets the time between commits.
     *
     * @deprecated use {@link ParallelConsumerOptions#setCommitInterval} instead. This will be deleted in the next major
     *         version.
     */
    // todo delete in next major version
    @Deprecated
    public Duration getTimeBetweenCommits() {
        return options.getCommitInterval();
    }

    /**
     * Sets the time between commits. Using a higher frequency will put more load on the brokers.
     *
     * @deprecated use {@link  ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval}} instead. This
     *         will be deleted in the next major version.
     */
    // todo delete in next major version
    @Deprecated
    public void setTimeBetweenCommits(final Duration timeBetweenCommits) {
        options.setCommitInterval(timeBetweenCommits);
    }

    /**
     * Under some conditions, waiting longer before committing can be faster
     *
     * @return true if waiting to commit would help performance
     */
    private boolean lingeringOnCommitWouldBeBeneficial() {
        // work is waiting to be done
        boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeProcessed();
        // no work is currently being done
        boolean workInFlight = wm.hasWorkInFlight();
        // work mailbox is empty
        boolean workWaitingInMailbox = !workMailbox.isEmpty();
        boolean workWaitingToProcess = wm.hasIncompleteOffsets();
        log.trace("workIsWaitingToBeCompletedSuccessfully {} || workInFlight {} || workWaitingInMailbox {} || !workWaitingToProcess {};",
                workIsWaitingToBeCompletedSuccessfully, workInFlight, workWaitingInMailbox, !workWaitingToProcess);
        boolean result = workIsWaitingToBeCompletedSuccessfully || workInFlight || workWaitingInMailbox || !workWaitingToProcess;

        // todo disable - commit frequency takes care of lingering? is this outdated?
        return false;
    }


    /**
     * The amount of time to block poll in this cycle
     *
     * @return either the duration until next commit, or next work retry
     * @see ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()
     */
    private Duration getTimeToBlockFor() {
        // if less than target work already in flight, don't sleep longer than the next retry time for failed work, if it exists - so that we can wake up and maybe retry the failed work
        if (!wm.isWorkInFlightMeetingTarget()) {
            // though check if we have work awaiting retry
            var lowestScheduledOpt = wm.getLowestRetryTime();
            if (lowestScheduledOpt.isPresent()) {
                // todo can sleep for less than this time? is this lower bound required? given that if we're starved - the failed work will most likely be selected? And even if not selected - then we will no longer be starved.
                Duration retryDelay = options.getDefaultMessageRetryDelay();
                // at min block for the retry time - retry time is not exact
                Duration lowestScheduled = lowestScheduledOpt.get();
                Duration timeBetweenCommits = getTimeBetweenCommits();
                Duration effectiveRetryDelay = lowestScheduled.toMillis() < retryDelay.toMillis() ? retryDelay : lowestScheduled;
                Duration result = timeBetweenCommits.toMillis() < effectiveRetryDelay.toMillis() ? timeBetweenCommits : effectiveRetryDelay;
                log.debug("Not enough work in flight, while work is waiting to be retried - so will only sleep until next retry time of {} (lowestScheduled = {})", result, lowestScheduled);
                return result;
            }
        }

        //
        Duration effectiveCommitAttemptDelay = getTimeToNextCommitCheck();
        log.debug("Calculated next commit time in {}", effectiveCommitAttemptDelay);
        return effectiveCommitAttemptDelay;
    }

    private Duration getTimeToNextCommitCheck() {
        // draining is a normal running mode for the controller
        if (state.isIdlingOrRunning()) {
            Duration timeSinceLastCommit = getTimeSinceLastCheck();
            Duration timeBetweenCommits = getTimeBetweenCommits();
            @SuppressWarnings("UnnecessaryLocalVariable")
            Duration minus = timeBetweenCommits.minus(timeSinceLastCommit);
            return minus;
        } else {
            log.debug("System not {} (state: {}), so don't wait to commit, only a small thread yield time", running, state);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCheck() {
        Instant now = module.clock().instant();
        return Duration.between(lastCommitCheckTime, now);
    }

    /**
     * Visible for testing
     */
    protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
        log.trace("Synchronizing on commitCommand...");
        synchronized (commitCommand) {
            log.debug("Committing offsets that are ready...");
            module.committer().retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
        }
    }

    private void clearCommitCommand() {
        synchronized (commitCommand) {
            if (commitCommand.get()) {
                log.debug("Command to commit asap received, clearing");
                this.commitCommand.set(false);
            }
        }
    }

    private <R> int retrieveAndDistributeNewWorkNew() {
        var capacity = workerPool.getCapacity(workRetrievalTimer);
        var work = wm.getWorkIfAvailable(capacity);
        workerPool.distribute(work);
        return work.size();
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     */
    public void addLoopEndCallBack(Runnable action) {
        this.controlLoopHooks.add(action);
    }

    public void awaitControlLoopClose(Duration timeout) {
        boolean interrupted = true;
        while (interrupted) {
            log.debug("Still interrupted");
            try {
                boolean terminationFinishedWithoutTimeout = getWorkerPool().awaitTermination(toSeconds(timeout)
                        , SECONDS);
                interrupted = false;
                if (!terminationFinishedWithoutTimeout) {
                    log.warn("Thread execution pool termination await timeout ({})! Were any processing jobs dead locked (test latch locks?) or otherwise stuck?", timeout);
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                interrupted = true;
            }
        }
        log.debug("Worker pool terminated.");
    }

}

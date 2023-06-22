package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.State.*;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @see ParallelConsumer
 */
@Slf4j
public abstract class AbstractParallelEoSStreamProcessor<K, V> implements ParallelConsumer<K, V>, ConsumerRebalanceListener, Closeable {

    public static final String MDC_INSTANCE_ID = "pcId";

    /**
     * Key for the work container descriptor that will be added to the {@link MDC diagnostic context} while inside a
     * user function.
     */
    private static final String MDC_WORK_CONTAINER_DESCRIPTOR = "offset";

    @Getter(PROTECTED)
    protected final ParallelConsumerOptions<K, V> options;

    /**
     * Injectable clock for testing
     */
    @Setter(AccessLevel.PACKAGE)
    private Clock clock = TimeUtils.getClock();

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

    private Instant lastCommitCheckTime = Instant.now();

    @Getter(PROTECTED)
    private final Optional<ProducerManager<K, V>> producerManager;

    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users' supplied function
     */
    protected final ThreadPoolExecutor workerThreadPool;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    // todo make package level
    @Getter(AccessLevel.PUBLIC)
    protected final WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    @Getter(PROTECTED)
    private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking

    private final AtomicBoolean isRebalanceInProgress = new AtomicBoolean(false);
    /**
     * An inbound message to the controller.
     * <p>
     * Currently, an Either type class, representing either newly polled records to ingest, or a work result.
     */
    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    private static class ControllerEventMessage<K, V> {

        WorkContainer<K, V> workContainer;

        EpochAndRecordsMap<K, V> consumerRecords;

        private boolean isWorkResult() {
            return workContainer != null;
        }

        private boolean isNewConsumerRecords() {
            return !isWorkResult();
        }

        private static <K, V> ControllerEventMessage<K, V> of(EpochAndRecordsMap<K, V> polledRecords) {
            return new ControllerEventMessage<>(null, polledRecords);
        }

        public static <K, V> ControllerEventMessage<K, V> of(WorkContainer<K, V> work) {
            return new ControllerEventMessage<K, V>(work, null);
        }
    }

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    /**
     * Useful for testing async code
     */
    private final List<Runnable> controlLoopHooks = new ArrayList<>();

    /**
     * Reference to the control thread, used for waking up a blocking poll ({@link BlockingQueue#poll}) against a
     * collection sooner.
     *
     * @see #processWorkCompleteMailBox
     */
    private Thread blockableControlThread;

    /**
     * @see #notifySomethingToDo
     * @see #processWorkCompleteMailBox
     */
    private final AtomicBoolean currentlyPollingWorkCompleteMailBox = new AtomicBoolean();

    private final OffsetCommitter committer;

    /**
     * Used to request a commit asap
     */
    private final AtomicBoolean commitCommand = new AtomicBoolean(false);

    /**
     * Multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} to have in our processing queue, in order to make
     * sure threads always have work to do.
     */
    protected final DynamicLoadFactor dynamicExtraLoadFactor;

    /**
     * If the system failed with an exception, it is referenced here.
     */
    private Exception failureReason;

    /**
     * Time of last successful commit
     */
    private Instant lastCommitTime;

    @Override
    public boolean isClosedOrFailed() {
        boolean closed = state == State.CLOSED;
        boolean doneOrCancelled = false;
        if (this.controlThreadFuture.isPresent()) {
            Future<Boolean> threadFuture = controlThreadFuture.get();
            doneOrCancelled = threadFuture.isDone() || threadFuture.isCancelled();
        }
        return closed || doneOrCancelled;
    }

    /**
     * @return if the system failed, returns the recorded reason.
     */
    public Exception getFailureCause() {
        return this.failureReason;
    }

    /**
     * The run state of the controller.
     *
     * @see State
     */
    @Setter
    private State state = State.UNUSED;

    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();

    @Getter
    private int numberOfAssignedPartitions;

    private final RateLimiter queueStatsLimiter = new RateLimiter();

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;

    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions) {
        this(newOptions, new PCModule<>(newOptions));
    }

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions, PCModule<K, V> module) {
        Objects.requireNonNull(newOptions, "Options must be supplied");

        options = newOptions;
        this.consumer = options.getConsumer();

        validateConfiguration();

        module.setParallelEoSStreamProcessor(this);

        log.info("Confluent Parallel Consumer initialise... groupId: {}, Options: {}",
                newOptions.getConsumer().groupMetadata().groupId(),
                newOptions);


        this.dynamicExtraLoadFactor = module.dynamicExtraLoadFactor();

        workerThreadPool = setupWorkerPool(newOptions.getMaxConcurrency());

        this.wm = module.workManager();

        this.brokerPollSubsystem = module.brokerPoller(this);

        if (options.isProducerSupplied()) {
            this.producerManager = Optional.of(module.producerManager());
            if (options.isUsingTransactionalProducer())
                this.committer = this.producerManager.get();
            else
                this.committer = this.brokerPollSubsystem;
        } else {
            this.producerManager = Optional.empty();
            this.committer = this.brokerPollSubsystem;
        }
    }

    private void validateConfiguration() {
        options.validate();

        checkGroupIdConfigured(consumer);
        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);
    }

    private void checkGroupIdConfigured(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            consumer.groupMetadata();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                    "configured GroupId on your Consumer?", e);
        }
    }

    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        ThreadFactory defaultFactory;
        try {
            defaultFactory = InitialContext.doLookup(options.getManagedThreadFactory());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            defaultFactory = Executors.defaultThreadFactory();
        }
        ThreadFactory finalDefaultFactory = defaultFactory;
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = finalDefaultFactory.newThread(r);
            String name = thread.getName();
            thread.setName("pc-" + name);
            this.getMyId().ifPresent(id -> thread.setName("pc-" + name + "-" + id));
            return thread;
        };
        ThreadPoolExecutor.AbortPolicy rejectionHandler = new ThreadPoolExecutor.AbortPolicy();
        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
    }

    private void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumerToCheck) {
        if (consumerToCheck instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumerToCheck.subscription();
        Set<TopicPartition> assignment = consumerToCheck.assignment();
        if (!subscription.isEmpty() || !assignment.isEmpty()) {
            throw new IllegalStateException("Consumer subscription must be managed by the Parallel Consumer. Use " + this.getClass().getName() + "#subcribe methods instead.");
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        log.debug("Subscribing to {}", topics);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern) {
        log.debug("Subscribing to {}", pattern);
        consumer.subscribe(pattern, this);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", topics);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", pattern);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(pattern, this);
    }

    /**
     * Commit our offsets
     * <p>
     * Make sure the calling thread is the thread which performs commit - i.e. is the {@link OffsetCommitter}.
     */
    @SneakyThrows
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("Partitions revoked {}, state: {}", partitions, state);
        isRebalanceInProgress.set(true);
        while (this.producerManager.map(ProducerManager::isTransactionCommittingInProgress).orElse(false))
            Thread.sleep(100); //wait for the transaction to finish committing

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            // commit any offsets from revoked partitions BEFORE truncation
            this.producerManager.ifPresent(pm -> {
                  try{
                        pm.preAcquireOffsetsToCommit();
                  } catch (Exception exc){
                        throw new InternalRuntimeException(exc);
                  }
            });

            commitOffsetsThatAreReady();

            // truncate the revoked partitions
            wm.onPartitionsRevoked(partitions);
        } catch (Exception e) {
            throw new InternalRuntimeException("onPartitionsRevoked event error", e);
        } finally {
            isRebalanceInProgress.set(false);
        }
        //
        try {
            usersConsumerRebalanceListener.ifPresent(listener -> listener.onPartitionsRevoked(partitions));
        } catch (Exception e) {
            throw new ExceptionInUserFunctionException("Error from rebalance listener function after #onPartitionsRevoked", e);
        }
    }

    /**
     * Delegate to {@link WorkManager}
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions + partitions.size();
        log.info("Assigned {} total ({} new) partition(s) {}", numberOfAssignedPartitions, partitions.size(), partitions);
        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions));
        notifySomethingToDo();
    }

    /**
     * Cannot commit any offsets for partitions that have been `lost` (as opposed to revoked). Just delegate to
     * {@link WorkManager} for truncation.
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();
        wm.onPartitionsLost(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsLost(partitions));
    }

    /**
     * Nasty reflection to check if auto commit is disabled.
     * <p>
     * Other way would be to politely request the user also include their consumer properties when construction, but
     * this is more reliable in a correctness sense, but brittle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    private void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            if (consumer instanceof KafkaConsumer) {
                // Could use Commons Lang FieldUtils#readField - but, avoid needing commons lang
                Field coordinatorField = KafkaConsumer.class.getDeclaredField("coordinator");
                coordinatorField.setAccessible(true);
                ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

                if (coordinator == null)
                    throw new IllegalStateException("Coordinator for Consumer is null - missing GroupId? Reflection broken?");

                Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
                autoCommitEnabledField.setAccessible(true);
                Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

                if (TRUE.equals(isAutoCommitEnabled))
                    throw new ParallelConsumerException("Consumer auto commit must be disabled, as commits are handled by the library.");
            } else if (consumer instanceof MockConsumer) {
                log.debug("Detected MockConsumer class which doesn't do auto commits");
            } else {
                // Probably Mockito
                log.error("Consumer is neither a KafkaConsumer nor a MockConsumer - cannot check auto commit is disabled for consumer type: " + consumer.getClass().getName());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Cannot check auto commit is disabled for consumer type: " + consumer.getClass().getName(), e);
        }
    }

    /**
     * Close the system, without draining.
     *
     * @see State#DRAINING
     */
    @Override
    public void close() {
        // use a longer timeout, to cover for evey other step using the default
        Duration timeout = DrainingCloseable.DEFAULT_TIMEOUT.multipliedBy(2);
        closeDontDrainFirst(timeout);
    }

    @Override
    @SneakyThrows
    public void close(Duration timeout, DrainingMode drainMode) {
        if (state == CLOSED) {
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
        while (!state.equals(CLOSED)) {
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

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException, InterruptedException {
        log.debug("Starting close process (state: {})...", state);

        log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerThreadPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done count: {}", unfinished.size());
        }

        log.debug("Awaiting worker pool termination...");
        boolean interrupted = true;
        while (interrupted) {
            log.debug("Still interrupted");
            try {
                boolean terminationFinishedWithoutTimeout = workerThreadPool.awaitTermination(toSeconds(timeout), SECONDS);
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

        // last check to see if after worker pool closed, has any new work arrived?
        processWorkCompleteMailBox(Duration.ZERO);

        //
        commitOffsetsThatAreReady();

        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        maybeCloseConsumer();

        producerManager.ifPresent(x -> x.close(timeout));

        log.debug("Close complete.");
        this.state = CLOSED;

        if (this.getFailureCause() != null) {
            log.error("PC closed due to error: {}", getFailureCause(), null);
        }
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumer() {
        if (isResponsibleForCommits()) {
            consumer.close();
        }
    }

    private boolean isResponsibleForCommits() {
        return (committer instanceof ProducerManager);
    }

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = wm.isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    private void transitionToDraining() {
        log.debug("Transitioning to draining...");
        this.state = State.DRAINING;
        notifySomethingToDo();
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to inform
     * that work is available when there was none, to make tests run faster, or to move on to shutting down the
     * {@link BrokerPollSystem} so that less messages are downloaded and queued.
     */
    private void interruptControlThread() {
        if (blockableControlThread != null) {
            log.debug("Interrupting {} thread in case it's waiting for work", blockableControlThread.getName());
            blockableControlThread.interrupt();
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

    /**
     * Optional ID of this instance. Useful for testing.
     */
    @Setter
    @Getter
    private Optional<String> myId = Optional.empty();

    /**
     * Kicks off the control loop in the executor, with supervision and returns.
     *
     * @see #supervisorLoop(Function, Consumer)
     */
    protected <R> void supervisorLoop(Function<PollContextInternal<K, V>, List<R>> userFunctionWrapped,
                                      Consumer<R> callback) {
        if (state != State.UNUSED) {
            throw new IllegalStateException(msg("Invalid state - you cannot call the poll* or pollAndProduce* methods " +
                    "more than once (they are asynchronous) (current state is {})", state));
        } else {
            state = RUNNING;
        }

        // broker poll subsystem
        brokerPollSubsystem.start(options.getManagedExecutorService());

        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup(options.getManagedExecutorService());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            executorService = Executors.newSingleThreadExecutor();
        }


        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            addInstanceMDC();
            log.info("Control loop starting up...");
            Thread controlThread = Thread.currentThread();
            controlThread.setName("pc-control");
            this.getMyId().ifPresent(id -> controlThread.setName("pc-control-" + id));
            this.blockableControlThread = controlThread;
            while (state != CLOSED) {
                log.debug("Control loop start");
                try {
                    controlLoop(userFunctionWrapped, callback);
                } catch (InterruptedException e) {
                    log.debug("Control loop interrupted, closing");
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT);
                } catch (Exception e) {
                    log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
                    failureReason = new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
                    throw failureReason;
                }
            }
            log.info("Control loop ending clean (state:{})...", state);
            return true;
        };
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    /**
     * Useful when testing with more than one instance
     */
    private void addInstanceMDC() {
        this.myId.ifPresent(id -> MDC.put(MDC_INSTANCE_ID, id));
    }

    /**
     * Main control loop
     */
    protected <R> void controlLoop(Function<PollContextInternal<K, V>, List<R>> userFunction,
                                   Consumer<R> callback) throws TimeoutException, ExecutionException, InterruptedException {
        maybeWakeupPoller();

        final boolean shouldTryCommitNow = maybeAcquireCommitLock();

        // make sure all work that's been completed are arranged ready for commit
        Duration timeToBlockFor = shouldTryCommitNow ? Duration.ZERO : getTimeToBlockFor();
        processWorkCompleteMailBox(timeToBlockFor);

        //
        if (shouldTryCommitNow) {
            // offsets will be committed when the consumer has its partitions revoked
            commitOffsetsThatAreReady();
        }

        // distribute more work
        retrieveAndDistributeNewWork(userFunction, callback);

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        log.trace("Current state: {}", state);
        switch (state) {
            case DRAINING -> {
                drain();
            }
            case CLOSING -> {
                doClose(DrainingCloseable.DEFAULT_TIMEOUT);
            }
        }

        // sanity - supervise the poller
        brokerPollSubsystem.supervise();

        // thread yield for spin lock avoidance
        Duration duration = Duration.ofMillis(1);
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            log.trace("Woke up", e);
        }

        // end of loop
        if (log.isTraceEnabled()) {
            log.trace("End of control loop, waiting processing {}, remaining in partition queues: {}, out for processing: {}. In state: {}",
                    wm.getNumberOfWorkQueuedInShardsAwaitingSelection(), wm.getNumberOfIncompleteOffsets(), wm.getNumberRecordsOutForProcessing(), state);
        }
    }

    /**
     * If we don't have enough work queued, and the poller is paused for throttling,
     * <p>
     * todo move into {@link WorkManager} as it's specific to WM having enough work?
     */
    private void maybeWakeupPoller() {
        if (state == RUNNING) {
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
        final boolean shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get();
        // could do this optimistically as well, and only get the lock if it's time to commit, so is not frequent
        if (shouldTryCommitNow && options.isUsingTransactionCommitMode()) {
            // get into write lock queue, so that no new work can be started from here on
            log.debug("Acquiring commit lock pessimistically, before we try to collect offsets for committing");
            //noinspection OptionalGetWithoutIsPresent - options will already be verified
            producerManager.get().preAcquireOffsetsToCommit();
        }
        return shouldTryCommitNow;
    }

    private <R> int retrieveAndDistributeNewWork(final Function<PollContextInternal<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        // check queue pressure first before addressing it
        checkPipelinePressure();

        int gotWorkCount = 0;

        //
        if (state == RUNNING || state == DRAINING) {
            int delta = calculateQuantityToRequest();
            var records = wm.getWorkIfAvailable(delta);

            gotWorkCount = records.size();
            lastWorkRequestWasFulfilled = gotWorkCount >= delta;

            log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }

        //
        queueStatsLimiter.performIfNotLimited(() -> {
            int queueSize = getNumberOfUserFunctionsQueued();
            log.debug("Stats: \n- pool active: {} queued:{} \n- queue size: {} target: {} loading factor: {}",
                    workerThreadPool.getActiveCount(), queueSize, queueSize, getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
        });

        return gotWorkCount;
    }

    /**
     * Submit a piece of work to the processing pool.
     *
     * @param workToProcess the polled records to process
     */
    protected <R> void submitWorkToPool(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                        Consumer<R> callback,
                                        List<WorkContainer<K, V>> workToProcess) {
        if (!workToProcess.isEmpty()) {
            log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), workerThreadPool);

            // perf: could inline makeBatches
            var batches = makeBatches(workToProcess);

            // debugging
            if (log.isDebugEnabled()) {
                var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
                log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
                List<Integer> integerStream = sizes.stream().filter(x -> x < (int) options.getBatchSize()).collect(Collectors.toList());
                if (integerStream.size() > 1) {
                    log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
                }
            }

            // submit
            for (var batch : batches) {
                submitWorkToPoolInner(usersFunction, callback, batch);
            }
        }
    }

    private <R> void submitWorkToPoolInner(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                           final Consumer<R> callback,
                                           final List<WorkContainer<K, V>> batch) {
        // for each record, construct dispatch to the executor and capture a Future
        log.trace("Sending work ({}) to pool", batch);
        Future outputRecordFuture = workerThreadPool.submit(() -> {
            addInstanceMDC();
            return runUserFunction(usersFunction, callback, batch);
        });
        // for a batch, each message in the batch shares the same result
        for (final WorkContainer<K, V> workContainer : batch) {
            workContainer.setFuture(outputRecordFuture);
        }
    }

    private List<List<WorkContainer<K, V>>> makeBatches(List<WorkContainer<K, V>> workToProcess) {
        int maxBatchSize = options.getBatchSize();
        return partition(workToProcess, maxBatchSize);
    }

    private static <T> List<List<T>> partition(Collection<T> sourceCollection, int maxBatchSize) {
        List<List<T>> listOfBatches = new ArrayList<>();
        List<T> batchInConstruction = new ArrayList<>();

        //
        for (T item : sourceCollection) {
            batchInConstruction.add(item);

            //
            if (batchInConstruction.size() == maxBatchSize) {
                listOfBatches.add(batchInConstruction);
                batchInConstruction = new ArrayList<>();
            }
        }

        // add partial tail
        if (!batchInConstruction.isEmpty()) {
            listOfBatches.add(batchInConstruction);
        }

        if (log.isDebugEnabled()) {
            log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                    sourceCollection.size(),
                    listOfBatches.size(),
                    listOfBatches.stream().map(List::size).collect(Collectors.toList()));
        }
        return listOfBatches;
    }

    /**
     * @return number of {@link WorkContainer} to try to get
     */
    protected int calculateQuantityToRequest() {
        int target = getTargetOutForProcessing();
        int current = wm.getNumberRecordsOutForProcessing();
        int delta = target - current;

        // always round up to fill batches - get however extra are needed to fill a batch
        if (options.isUsingBatching()) {
            //noinspection OptionalGetWithoutIsPresent
            int batchSize = options.getBatchSize();
            int modulo = delta % batchSize;
            if (modulo > 0) {
                int extraToFillBatch = target - modulo;
                delta = delta + extraToFillBatch;
            }
        }

        log.debug("Will try to get work - target: {}, current queue size: {}, requesting: {}, loading factor: {}",
                target, current, delta, dynamicExtraLoadFactor.getCurrentFactor());
        return delta;
    }

    protected int getTargetOutForProcessing() {
        return getQueueTargetLoaded();
    }

    protected int getQueueTargetLoaded() {
        //noinspection unchecked
        int batch = options.getBatchSize();
        return getPoolLoadTarget() * dynamicExtraLoadFactor.getCurrentFactor() * batch;
    }

    /**
     * Checks the system has enough pressure in the pipeline of work, if not attempts to step up the load factor.
     */
    protected void checkPipelinePressure() {
        if (log.isTraceEnabled())
            log.trace("Queue pressure check: (current size: {}, loaded target: {}, factor: {}) " +
                            "if (isPoolQueueLow() {} && lastWorkRequestWasFulfilled {}))",
                    getNumberOfUserFunctionsQueued(),
                    getQueueTargetLoaded(),
                    dynamicExtraLoadFactor.getCurrentFactor(),
                    isPoolQueueLow(),
                    lastWorkRequestWasFulfilled);

        if (isPoolQueueLow() && lastWorkRequestWasFulfilled) {
            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();
            if (steppedUp) {
                log.debug("isPoolQueueLow(): Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        getNumberOfUserFunctionsQueued(), getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}", dynamicExtraLoadFactor.getCurrentFactor(), dynamicExtraLoadFactor.getMaxFactor());
            }
        }
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolLoadTarget() {
        return options.getTargetAmountOfRecordsInFlight();
    }

    private boolean isPoolQueueLow() {
        int queueSize = getNumberOfUserFunctionsQueued();
        int queueTarget = getPoolLoadTarget();
        boolean workAmountBelowTarget = queueSize <= queueTarget;
        log.debug("isPoolQueueLow()? workAmountBelowTarget {} {} vs {};",
                workAmountBelowTarget, queueSize, queueTarget);
        return workAmountBelowTarget;
    }

    private void drain() {
        log.debug("Signaling to drain...");
        brokerPollSubsystem.drain();
        if (!isRecordsAwaitingProcessing()) {
            transitionToClosing();
        } else {
            log.debug("Records still waiting processing, won't transition to closing.");
        }
    }

    private void transitionToClosing() {
        log.debug("Transitioning to closing...");
        if (state == State.UNUSED) {
            state = CLOSED;
        } else {
            state = State.CLOSING;
        }
        notifySomethingToDo();
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing.
     * <p>
     * Visible for testing.
     */
    protected void processWorkCompleteMailBox(final Duration timeToBlockFor) {
        log.trace("Processing mailbox (might block waiting for results)...");
        Queue<ControllerEventMessage<K, V>> results = new ArrayDeque<>();

        if (timeToBlockFor.toMillis() > 0) {
            currentlyPollingWorkCompleteMailBox.getAndSet(true);
            if (log.isDebugEnabled()) {
                log.debug("Blocking poll on work until next scheduled offset commit attempt for {}. active threads: {}, queue: {}",
                        timeToBlockFor, workerThreadPool.getActiveCount(), getNumberOfUserFunctionsQueued());
            }
            // wait for work, with a timeToBlockFor for sanity
            log.trace("Blocking poll {}", timeToBlockFor);
            try {
                var firstBlockingPoll = workMailBox.poll(timeToBlockFor.toMillis(), MILLISECONDS);
                if (firstBlockingPoll == null) {
                    log.debug("Mailbox results returned null, indicating timeToBlockFor elapsed (which was set as {})", timeToBlockFor);
                } else {
                    log.debug("Work arrived in mailbox during blocking poll. (Timeout was set as {})", timeToBlockFor);
                    results.add(firstBlockingPoll);
                }
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting on work results");
            } finally {
                currentlyPollingWorkCompleteMailBox.getAndSet(false);
            }
            log.trace("Blocking poll finish");
        }

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = workMailBox.size();
        log.trace("Draining {} more, got {} already...", size, results.size());
        workMailBox.drainTo(results, size);

        log.trace("Processing drained work {}...", results.size());
        for (var action : results) {
            if (action.isNewConsumerRecords()) {
                wm.registerWork(action.getConsumerRecords());
            } else {
                WorkContainer<K, V> work = action.getWorkContainer();
                MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, work.toString());
                wm.handleFutureResult(work);
                MDC.remove(MDC_WORK_CONTAINER_DESCRIPTOR);
            }
        }
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

    private boolean isIdlingOrRunning() {
        return state == RUNNING || state == DRAINING || state == PAUSED;
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

    private int getNumberOfUserFunctionsQueued() {
        return workerThreadPool.getQueue().size();
    }

    /**
     * Under some conditions, waiting longer before committing can be faster
     *
     * @return true if waiting to commit would help performance
     */
    private boolean lingeringOnCommitWouldBeBeneficial() {
        if (log.isTraceEnabled()) {
            // work is waiting to be done
            boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeProcessed();
            // no work is currently being done
            boolean workInFlight = wm.hasWorkInFlight();
            // work mailbox is empty
            boolean workWaitingInMailbox = !workMailBox.isEmpty();
            boolean workWaitingToProcess = wm.hasIncompleteOffsets();
            log.trace("workIsWaitingToBeCompletedSuccessfully {} || workInFlight {} || workWaitingInMailbox {} || !workWaitingToProcess {};",
                    workIsWaitingToBeCompletedSuccessfully, workInFlight, workWaitingInMailbox, !workWaitingToProcess);
            boolean result = workIsWaitingToBeCompletedSuccessfully || workInFlight || workWaitingInMailbox || !workWaitingToProcess;
        }

        // todo disable - commit frequency takes care of lingering? is this outdated?
        return false;
    }

    private Duration getTimeToNextCommitCheck() {
        // draining is a normal running mode for the controller
        if (isIdlingOrRunning()) {
            Duration timeSinceLastCommit = getTimeSinceLastCheck();
            Duration timeBetweenCommits = getTimeBetweenCommits();
            @SuppressWarnings("UnnecessaryLocalVariable")
            Duration minus = timeBetweenCommits.minus(timeSinceLastCommit);
            return minus;
        } else {
            log.debug("System not {} (state: {}), so don't wait to commit, only a small thread yield time", RUNNING, state);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCheck() {
        Instant now = clock.instant();
        return Duration.between(lastCommitCheckTime, now);
    }

    /**
     * Visible for testing
     */
    protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
        log.trace("Synchronizing on commitCommand...");
        synchronized (commitCommand) {
            log.debug("Committing offsets that are ready...");
            committer.retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
        }
    }

    private void updateLastCommitCheckTime() {
        lastCommitCheckTime = Instant.now();
    }

    /**
     * Run the supplied function.
     */
    protected <R> List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                        Consumer<R> callback,
                                                                                        List<WorkContainer<K, V>> workContainerBatch) {
        if (log.isDebugEnabled()) {
            // first offset of the batch
            MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, workContainerBatch.get(0).offset() + "");
        }
        log.trace("Pool received: {}", workContainerBatch);

        /*
         *  Handle stale work from the batch, before creating the internal context for running user function.
         *  The context created is used by the "wrapped" user function to inject transactional producer synchronization.
         */
        final boolean containsStaleWork = wm.checkIfWorkIsStale(workContainerBatch);

        if (containsStaleWork) {
            handleStaleWork(workContainerBatch);
        }

        final List<WorkContainer<K, V>> activeWorkContainers = containsStaleWork ?
                workContainerBatch
                        .stream()
                        .filter(wc -> !wm.checkIfWorkIsStale(wc))
                        .collect(Collectors.toList())
                : workContainerBatch;

        final PollContextInternal<K, V> context = new PollContextInternal<>(activeWorkContainers);

        try {
            if (!activeWorkContainers.isEmpty()) {
                return runUserFunctionInternal(usersFunction, context, callback, activeWorkContainers);
            }
            return Collections.emptyList();
        } catch (Exception e) {
            // handle fail
            var cause = e.getCause();
            String msg = msg("Exception caught in user function running stage, registering WC as failed, returning to" +
                    " mailbox. Context: {}", context, e);
            if (cause instanceof PCRetriableException) {
                log.debug("Explicit " + PCRetriableException.class.getSimpleName() + " caught, logging at DEBUG only. " + msg, e);
            } else {
                log.error(msg, e);
            }

            for (var wc : workContainerBatch) {
                wc.onUserFunctionFailure(e);
                addToMailbox(context, wc); // always add on error
            }
            throw e; // trow again to make the future failed
        } finally {
            cleanUpContext(context);
        }
    }

    /**
     * Given the batch of work containers, publish stale work to feedback loop to be reduced from in progress work.
     *
     * @param workContainerBatch
     */
    protected void handleStaleWork(final List<WorkContainer<K, V>> workContainerBatch) {
        final List<WorkContainer<K, V>> staleWorkContainers = workContainerBatch
                .stream()
                .filter(wm::checkIfWorkIsStale)
                .collect(Collectors.toList());
        final PollContextInternal<K, V> internalContext = new PollContextInternal<>(staleWorkContainers);
        try {
            // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
            log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", staleWorkContainers);
            staleWorkContainers.forEach(wc -> addToMailbox(internalContext, wc));
        } finally {
            cleanUpContext(internalContext);
        }
    }

    protected <R> ArrayList<Tuple<ConsumerRecord<K, V>, R>> runUserFunctionInternal(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                    final PollContextInternal<K, V> context,
                                                                                    final Consumer<R> callback,
                                                                                    final List<WorkContainer<K, V>> activeWorkContainers) {
        List<R> resultsFromUserFunction;
        resultsFromUserFunction = usersFunction.apply(context);

        for (final WorkContainer<K, V> kvWorkContainer : activeWorkContainers) {
            onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
        }

        // capture each result, against the input record
        var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
        for (R result : resultsFromUserFunction) {
            log.trace("Running users call back...");
            callback.accept(result);
        }

        // fail or succeed, either way we're done
        for (var kvWorkContainer : activeWorkContainers) {
            addToMailBoxOnUserFunctionSuccess(context, kvWorkContainer, resultsFromUserFunction);
        }
        log.trace("User function future registered");

        return intermediateResults;
    }

    private void cleanUpContext(final PollContextInternal<K, V> context) {
        context.getProducingLock().ifPresent(ProducerManager.ProducingLock::unlock);
    }

    protected void addToMailBoxOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        addToMailbox(context, wc);
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    protected void addToMailbox(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc) {
        String state = wc.isUserFunctionSucceeded() ? "succeeded" : "FAILED";
        log.trace("Adding {} {} to mailbox...", state, wc);
        workMailBox.add(ControllerEventMessage.of(wc));

        wc.onPostAddToMailBox(pollContext, producerManager);
    }

    public void registerWork(EpochAndRecordsMap<K, V> polledRecords) {
        log.trace("Adding {} to mailbox...", polledRecords);
        workMailBox.add(ControllerEventMessage.of(polledRecords));
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while polling the mail box.
     *
     * @see #processWorkCompleteMailBox
     * @see #blockableControlThread
     */
    public void notifySomethingToDo() {
        boolean noTransactionInProgress = !producerManager.map(ProducerManager::isTransactionCommittingInProgress).orElse(false);
        if (noTransactionInProgress) {
            log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
            interruptControlThread();
        } else {
            log.trace("Would have interrupted control thread, but TX in progress");
        }
    }

    @Override
    public long workRemaining() {
        return wm.getNumberOfIncompleteOffsets();
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     */
    public void addLoopEndCallBack(Runnable r) {
        this.controlLoopHooks.add(r);
    }

    public void setLongPollTimeout(Duration ofMillis) {
        BrokerPollSystem.setLongPollTimeout(ofMillis);
    }

    /**
     * Request a commit as soon as possible (ASAP), overriding other constraints.
     */
    public void requestCommitAsap() {
        log.debug("Registering command to commit next chance");
        synchronized (commitCommand) {
            this.commitCommand.set(true);
        }
        notifySomethingToDo();
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            return this.commitCommand.get();
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

    @Override
    public void pauseIfRunning() {
        if (this.state == State.RUNNING) {
            log.info("Transitioning parallel consumer to state paused.");
            this.state = State.PAUSED;
        } else {
            log.debug("Skipping transition of parallel consumer to state paused. Current state is {}.", this.state);
        }
    }

    @Override
    public void resumeIfPaused() {
        if (this.state == State.PAUSED) {
            log.info("Transitioning parallel consumer to state running.");
            this.state = State.RUNNING;
            notifySomethingToDo();
        } else {
            log.debug("Skipping transition of parallel consumer to state running. Current state is {}.", this.state);
        }
    }

}

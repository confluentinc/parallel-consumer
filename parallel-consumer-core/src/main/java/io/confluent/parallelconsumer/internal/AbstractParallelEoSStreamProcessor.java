package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.actors.Actor;
import io.confluent.csid.actors.ActorImpl;
import io.confluent.csid.actors.Interruptible.Reason;
import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
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
import static lombok.AccessLevel.PROTECTED;
import static lombok.AccessLevel.PUBLIC;

/**
 * @see ParallelConsumer
 */
@Slf4j
public abstract class AbstractParallelEoSStreamProcessor<K, V> extends RebalanceHandler implements
        ParallelConsumer<K, V>,
        ControllerInternalAPI<K, V>,
        Closeable {

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

    protected ControllerInternalAPI<K, V> controllerApi;

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

    /**
     * Actor for IPC
     */
    // todo make private
    @Getter(PROTECTED)
    private final Actor<AbstractParallelEoSStreamProcessor<K, V>> myActor = new ActorImpl<>(this);

    @Getter(PROTECTED)
    private final Optional<ProducerManager<K, V>> producerManager;

    // todo remove with consumer facade - branch improvements/consumer-interface
    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users' supplied function
     */
    protected final ThreadPoolExecutor workerThreadPool;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    // todo make package level - in controller extraction branch
    @Getter(PUBLIC)
    protected final WorkManager<K, V> wm;

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    private final RebalanceHandler rebalanceHandler;

    /**
     * Useful for testing async code
     */
    private final List<Runnable> controlLoopHooks = new ArrayList<>();

    private final OffsetCommitter committer;

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
    private State state = UNUSED;

    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    // todo this needs to be reconciled with all subscription flows
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();

    private final RateLimiter queueStatsLimiter = new RateLimiter();

    private final RateLimiter maxLoadingLog = new RateLimiter(30);

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
        super(module.consumer());

        Objects.requireNonNull(newOptions, "Options must be supplied");

        controllerApi = this;

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

        this.rebalanceHandler = module.parallelEoSStreamProcessor;

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
    public void subscribe(String topic) {
        subscribe(Collections.singletonList(topic));
    }

    // replace with facade delegate
    @Override
    public void subscribe(Collection<String> topics) {
        log.debug("Subscribing to {}", topics);
        consumer.subscribe(topics, this.rebalanceHandler);
    }

    @Override
    public void subscribe(Pattern pattern) {
        log.debug("Subscribing to {}", pattern);
        consumer.subscribe(pattern, this.rebalanceHandler);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", topics);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(topics, this.rebalanceHandler);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", pattern);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(pattern, this.rebalanceHandler);
    }

    protected void onPartitionsRevokedTellAsync(Collection<TopicPartition> partitions) {
        getMyActor().tellImmediately(controller -> controller.onPartitionsRevokedInternal(partitions));
    }

    /**
     * Commit our offsets
     * <p>
     * Make sure the calling thread is the thread which performs commit - i.e. is the {@link OffsetCommitter}.
     */
    private void onPartitionsRevokedInternal(Collection<TopicPartition> partitions) {
        log.debug("Partitions revoked {}, state: {}", partitions, state);
        try {
            // commit any offsets from revoked partitions BEFORE truncation
            commitOffsetsThatAreReady();

            // truncate the revoked partitions
            wm.onPartitionsRevoked(partitions);
        } catch (Exception e) {
            throw new InternalRuntimeException("onPartitionsRevoked event error", e);
        }

        //
        try {
            usersConsumerRebalanceListener.ifPresent(listener -> listener.onPartitionsRevoked(partitions));
        } catch (Exception e) {
            throw new ExceptionInUserFunctionException("Error from rebalance listener function after #onPartitionsRevoked", e);
        }
    }

    @Override
    protected void onPartitionsAssignedTellAsync(CommitData partitions) {
        getMyActor().tellImmediately(controller -> controller.onPartitionsAssignedInternal(partitions));
    }

    /**
     * Delegate to {@link WorkManager}
     *
     * @see WorkManager#onPartitionsAssigned
     */
    private void onPartitionsAssignedInternal(CommitData partitions) {
        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions.keySet()));
        // todo interrupting can be removed after improvements/rebalance-messages is merged
        notifySomethingToDo(new Reason("New partitions assigned"));
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
    @SneakyThrows
    @ThreadSafe
    @Override
    public void close() {
        // use a longer timeout, to cover for evey other step using the default
        Duration timeout = DrainingCloseable.DEFAULT_TIMEOUT.multipliedBy(2);
        closeDontDrainFirst(timeout);
    }

    @ThreadSafe
    @Override
    public void close(Duration timeout, DrainingMode drainMode) throws ExecutionException, TimeoutException, InterruptedException {
        try {
            if (isIdlingOrRunning()) {
                var close = getMyActor().tellImmediatelyWithAck(me
                        -> me.closeInternal(drainMode));
                // wait - blocks
                close.get(timeout.toMillis(), MILLISECONDS);
            } else {
                // control loop not running, perform the close directly
                closeResources(timeout);
            }

            processCloseState(timeout);
        } catch (InterruptedException e) {
            // ignore
            log.debug("Interrupted waiting on close...", e);
            throw e;
        } catch (ExecutionException | TimeoutException e) {
            log.error("Execution or timeout exception while waiting for the control thread to close cleanly " +
                    "(state was {}). Try increasing your time-out to allow the system to drain, or close without " +
                    "draining.", state, e);
            throw e;
        }
    }

    private void processCloseState(Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
        if (controlThreadFuture.isPresent()) {
            log.debug("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(timeout.toMillis(), MILLISECONDS); // throws exception if supervisor saw one
        }

        log.info("Close complete.");
    }

    private void closeInternal(DrainingMode drainMode) {
        if (state == CLOSED) {
            log.info("Already closed");
        } else {
            log.info("Signaling to close...");

            switch (drainMode) {
                case DRAIN -> {
                    log.info("Will wait for all in flight to complete before");
                    transitionToDraining(new Reason("Closing"));
                }
                case DONT_DRAIN -> {
                    log.info("Not waiting for remaining queued to complete, will finish in flight, then close...");
                    transitionToClosing();
                }
            }
        }
    }

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException, InterruptedException {
        log.debug("Starting close process (state: {})...", state);

        cleanup(timeout);

        closeResources(timeout);

        //
        log.debug("Close complete.");
        this.state = CLOSED;

        if (this.getFailureCause() != null) {
            log.error("PC closed due to error: {}", getFailureCause(), null);
        }
    }

    private void cleanup(Duration timeout) throws InterruptedException, TimeoutException {
        log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerThreadPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done count: {}", unfinished.size());
        }

        log.debug("Awaiting worker pool termination...");
        // todo with new actor / interrupt process, is interrupted test still needed? - try remove
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
                log.debug("Interrupted waiting on worker threads to close, retrying...", e);
                interrupted = true;
            }
        }
        log.debug("Worker pool terminated.");

        // last check to see if after worker pool closed, has any new work arrived?
        processActorMessageQueueBlocking();

        // last commit
        commitOffsetsThatAreReady();
    }

    private void closeResources(Duration timeout) throws TimeoutException, ExecutionException {
        // only broker poller, and hence consumer, once `Committer` has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        // reject further and process renaming - make sure no new messages possible
        // todo can't close until when? to process responses? so then come public apis need to check state first before
        //  allowing message? stop actor first, then process, then close?
        getMyActor().close();

        //
        maybeCloseConsumer();

        //
        producerManager.ifPresent(x -> x.close(timeout));
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

    private void transitionToDraining(Reason reason) {
        transitionToState(reason, State.DRAINING);
    }

    private void transitionToState(Reason reason, State newState) {
        log.debug("Transitioning to state {} from {} - reason: {}...", reason, newState, state);
        this.state = newState;
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
        if (state != UNUSED) {
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
            Thread controlThread = Thread.currentThread();
            controlThread.setName("pc-control");
            addInstanceMDC();
            log.info("Control loop starting up...");
            while (state != CLOSED) {
                log.debug("Control loop start");
                try {
                    controlLoop(userFunctionWrapped, callback);
                } catch (InterruptedException e) {
                    log.debug("Control loop interrupted, closing");
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT);
                } catch (Exception e) {
                    log.error(msg("Error from poll control thread, will attempt controlled shutdown, then rethrow. " +
                            "Error: " + e.getMessage()), e);
                    transitionToClosing();
                    failureReason = new RuntimeException(msg("Error from poll control thread: " + e.getMessage()), e);
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
                    throw failureReason;
                }
            }
            log.info("Control loop ending clean (state:{})...", state);
            return true;
        };
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);

        getMyActor().start();
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

        //
        final boolean shouldTryCommitNow = maybeAcquireCommitLock();

        log.trace("Loop: Process actor queue");
        try {
            processActorMessageQueueBlocking();
        } catch (InterruptedException e) {
            log.warn("Interrupted processing work in control loop, skipping...");
            Thread.currentThread().interrupt();
        }

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

        // end of loop
        log.trace("End of control loop, waiting processing {}, remaining in partition queues: {}, out for processing: {}. In state: {}",
                wm.getTotalSizeOfAllShards(), wm.getNumberOfIncompleteOffsets(), wm.getNumberRecordsOutForProcessing(), state);
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
                        wm.getTotalSizeOfAllShards(),
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

        log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                sourceCollection.size(),
                listOfBatches.size(),
                listOfBatches.stream().map(List::size).collect(Collectors.toList()));
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
        if (log.isTraceEnabled()) {
            log.trace("Queue pressure check: (current size: {}, loaded target: {}, factor: {}) " +
                            "if (isPoolQueueLow() {} && lastWorkRequestWasFulfilled {}))",
                    getNumberOfUserFunctionsQueued(),
                    getQueueTargetLoaded(),
                    dynamicExtraLoadFactor.getCurrentFactor(),
                    isPoolQueueLow(),
                    lastWorkRequestWasFulfilled);
        }

        if (isPoolQueueLow() && lastWorkRequestWasFulfilled) {
            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();
            if (steppedUp) {
                log.debug("isPoolQueueLow(): Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        getNumberOfUserFunctionsQueued(), getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                maxLoadingLog.performIfNotLimited(()
                        -> log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}",
                        dynamicExtraLoadFactor.getCurrentFactor(),
                        dynamicExtraLoadFactor.getMaxFactor()));
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
        if (state == UNUSED) {
            state = CLOSED;
        } else {
            state = CLOSING;
        }
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing via the {@link Actor}, {@link #myActor}.
     * <p>
     * Visible for testing.
     */
    private void processActorMessageQueueBlocking() throws InterruptedException {
        Duration timeToBlockFor = calculateTimeUntilNextAction();
        getMyActor().processBlocking(timeToBlockFor);
    }

    private void handleWorkResult(WorkContainer<K, V> work) {
        MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, work.toString());
        wm.handleFutureResult(work);
        MDC.remove(MDC_WORK_CONTAINER_DESCRIPTOR);
    }

    /**
     * The amount of time to block poll in this cycle
     *
     * @return either the duration until next commit, or next work retry
     * @see ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()
     */
    private Duration calculateTimeUntilNextAction() {
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
        log.debug("Time to block until next action calculated as {}", effectiveCommitAttemptDelay);
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

        boolean shouldDoANormalCommit = commitFrequencyOK && !lingerBeneficial;

        boolean shouldCommitNow = shouldDoANormalCommit;

        if (log.isDebugEnabled()) {
            log.debug("Should commit this cycle? " +
                    "shouldCommitNow? " + shouldCommitNow + " : " +
                    "shouldDoANormalCommit? " + shouldDoANormalCommit + ", " +
                    "commitFrequencyOK? " + commitFrequencyOK + ", " +
                    "lingerBeneficial? " + lingerBeneficial
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
    @Deprecated // todo check?
    private boolean lingeringOnCommitWouldBeBeneficial() {
        // work is waiting to be done
        boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeProcessed();
        // no work is currently being done
        boolean workInFlight = wm.hasWorkInFlight();
        // work actor queue is empty
        boolean workWaitingInActorQueue = !getMyActor().isEmpty();
        boolean workWaitingToProcess = wm.hasIncompleteOffsets();
        log.trace("workIsWaitingToBeCompletedSuccessfully {} || workInFlight {} || workWaitingInActorQueue {} || !workWaitingToProcess {};",
                workIsWaitingToBeCompletedSuccessfully, workInFlight, workWaitingInActorQueue, !workWaitingToProcess);
        boolean result = workIsWaitingToBeCompletedSuccessfully || workInFlight || workWaitingInActorQueue || !workWaitingToProcess;

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
    protected void commitOffsetsThatAreReady() throws InterruptedException, TimeoutException {
        if (state.equals(UNUSED)) {
            return;
        }

        log.debug("Committing offsets that are ready...");
        var commitData = wm.collectCommitDataForDirtyPartitions();
        committer.retrieveOffsetsAndCommit(commitData);
        updateLastCommitCheckTime();
        this.lastCommitTime = Instant.now();
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
        // call the user's function
        List<R> resultsFromUserFunction;
        PollContextInternal<K, V> context = new PollContextInternal<>(workContainerBatch);

        try {
            if (log.isDebugEnabled()) {
                // first offset of the batch
                MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, workContainerBatch.get(0).offset() + "");
            }
            log.trace("Pool received: {}", workContainerBatch);

            //
            boolean workIsStale = wm.checkIfWorkIsStale(workContainerBatch);
            if (workIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", workContainerBatch);
                return null;
            }

            resultsFromUserFunction = usersFunction.apply(context);

            for (final WorkContainer<K, V> kvWorkContainer : workContainerBatch) {
                onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
            }

            // capture each result, against the input record
            var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
            for (R result : resultsFromUserFunction) {
                log.trace("Running users call back...");
                callback.accept(result);
            }

            // fail or succeed, either way we're done
            for (var kvWorkContainer : workContainerBatch) {
                addWorkResultOnUserFunctionSuccess(context, kvWorkContainer, resultsFromUserFunction);
            }
            log.trace("User function future registered");

            return intermediateResults;
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
                controllerApi.sendWorkResultAsync(context, wc); // always add on error
            }
            throw e; // trow again to make the future failed
        } finally {
            context.getProducingLock().ifPresent(ProducerManager.ProducingLock::unlock);
        }
    }

    /**
     * @param resultsFromUserFunction not used in this implementation
     * @see ExternalEngine#onUserFunctionSuccess
     * @see ExternalEngine#isAsyncFutureWork
     */
    // todo collapse after controller refactor
    protected void addWorkResultOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) { // NOSONAR
        controllerApi.sendWorkResultAsync(context, wc);
    }

    /**
     * @param resultsFromUserFunction not used in this implementation
     * @see ExternalEngine#onUserFunctionSuccess
     * @see ExternalEngine#isAsyncFutureWork
     */
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) { // NOSONAR
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    // todo make protected - after controller refactor - it won't be part of the main public class anymore
    @ThreadSafe
    @Override
    public void sendWorkResultAsync(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc) {
        log.trace("Sending new work result to controller {}", wc);
        getMyActor().tell(controller -> controller.handleWorkResult(wc));

        wc.onPostAddToMailBox(pollContext, producerManager);
    }

    // todo make protected - after controller refactor - it won't be part of the main public class anymore
    @ThreadSafe
    @Override
    public void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords) {
        log.trace("Sending new polled records signal to controller - total partitions: {} records: {}",
                polledRecords.partitions().size(),
                polledRecords.count());
        getMyActor().tell(controller -> controller.getWm().registerWork(polledRecords));
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while performing {@link Actor#processBlocking}
     *
     * @see #processActorMessageQueueBlocking
     * @deprecated todo should be defunct after full migration to Actor framework, as ANY messages for the controller will also wake it up.
     */
    @Deprecated
    public void notifySomethingToDo(Reason reason) {
        // todo reason enum? extend? e.g. Reason.COMMIT_TIME ?
        getMyActor().interrupt(reason);
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
     * <p>
     * Useful for testing, but otherwise the close methods will commit and clean up properly.
     */
    // todo in controller refactor - consider making this part of the user API, or hidden
    public void requestCommitAsap() {
        // todo consider making this use a trigger variable like is used to - as we may end up committing twice right after another
        log.debug("Registering command to commit next chance");
        // if want immediate commit, need to wake up poller here too - call #commitOffsetsThatAreReadyImmediately instead
        getMyActor().tell(this::commitOffsetsThatAreReadyWrapped);
    }

    @SneakyThrows
    private void commitOffsetsThatAreReadyWrapped(AbstractParallelEoSStreamProcessor<K, V> controller) {
        controller.commitOffsetsThatAreReady();
    }

    @ThreadSafe
    @Override
    public void pauseIfRunning() {
        getMyActor().tellImmediately(me -> {
            if (this.state == State.RUNNING) {
                log.info("Transitioning parallel consumer to state paused.");
                this.state = State.PAUSED;
            } else {
                log.debug("Skipping transition of parallel consumer to state paused. Current state is {}.", this.state);
            }
        });
    }

    @ThreadSafe
    @Override
    public void resumeIfPaused() {
        getMyActor().tellImmediately(me -> {
            if (this.state == State.PAUSED) {
                log.info("Transitioning parallel consumer to state running.");
                transitionToState(new Reason("Resuming"), RUNNING);
            } else {
                log.debug("Skipping transition of parallel consumer to state running. Current state is {}.", this.state);
            }
        });
    }

    // todo remove - for tests that call controlLoop directly
    public void start() {
        getMyActor().start();
    }
}

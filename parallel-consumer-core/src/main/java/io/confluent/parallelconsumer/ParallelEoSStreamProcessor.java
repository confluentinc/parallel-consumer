package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.WallClock;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.MDC;
import pl.tlinkowski.unij.api.UniLists;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.confluent.parallelconsumer.ParallelEoSStreamProcessor.State.*;
import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.UserFunctions.carefullyRun;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @see ParallelConsumer
 */
@Slf4j
public class ParallelEoSStreamProcessor<K, V> implements ParallelStreamProcessor<K, V>, ConsumerRebalanceListener, Closeable {

    private final ParallelConsumerOptions options;

//    private final BackoffAnalyser backoffer;

    /**
     * Injectable clock for testing
     */
    @Setter(AccessLevel.PACKAGE)
    private WallClock clock = new WallClock();

    @Setter
    @Getter
    private Duration timeBetweenCommits = ofMillis(500);

    private Instant lastCommit = Instant.now();

    private final Optional<ProducerManager<K, V>> producerManager;

    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users's supplied function
     */
    private final ThreadPoolExecutor workerPool;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    @Getter(AccessLevel.PACKAGE)
    protected WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    @Getter
    private final BlockingQueue<WorkContainer<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking

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
     * @see #notifyNewWorkRegistered
     * @see #processWorkCompleteMailBox
     */
    private final AtomicBoolean currentlyPollingWorkCompleteMailBox = new AtomicBoolean();

    private final OffsetCommitter committer;

    /**
     * Used to request a commit asap
     */
    private final AtomicBoolean commitCommand = new AtomicBoolean(false);
    private final DynamicLoadFactor dynamicExtraLoadFactor = new DynamicLoadFactor();

    public boolean isClosedOrFailed() {
        boolean closed = state == State.closed;
        boolean doneOrCancelled = false;
        if (this.controlThreadFuture.isPresent()) {
            doneOrCancelled = controlThreadFuture.get().isDone() || controlThreadFuture.get().isCancelled();
        }
        return closed || doneOrCancelled;
    }

    /**
     * The run state of the controller.
     *
     * @see #state
     */
    enum State {
        unused,
        running,
        /**
         * When draining, the system will stop polling for more records, but will attempt to process all already
         * downloaded records. Note that if you choose to close without draining, records already processed will still
         * be committed first before closing.
         *
         * @see #closeDrainFirst()
         * @see #close()
         */
        draining,
        closing,
        closed;
    }

    /**
     * The run state of the controller.
     *
     * @see State
     */
    private State state = State.unused;

    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();

    BlockingQueue<Runnable> ringbufferRunnables;
    RingBufferManager<K, V> rb;

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    public ParallelEoSStreamProcessor(ParallelConsumerOptions newOptions) {
        Objects.requireNonNull(newOptions, "Options must be supplied");

        log.info("Confluent Parallel Consumer initialise... Options: {}", newOptions);

        options = newOptions;
        options.validate();

//        this.backoffer = new BackoffAnalyser();

        this.consumer = options.getConsumer();

        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);

        LinkedBlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>();
        ArrayBlockingQueue<WorkContainer<K, V>> ringbuffer = new ArrayBlockingQueue<>(options.getNumberOfThreads() * dynamicExtraLoadFactor.getCurrent());
        ringbufferRunnables = new ArrayBlockingQueue<>(options.getNumberOfThreads() * dynamicExtraLoadFactor.getCurrent());
        ringbufferRunnables = new LinkedBlockingQueue<>();

        workerPool = new ThreadPoolExecutor(newOptions.getNumberOfThreads(), newOptions.getNumberOfThreads(),
                0L, MILLISECONDS,
                ringbufferRunnables)
        ;
//        {
//            @Override
//            protected void beforeExecute(final Thread t, final Runnable r) {
//                super.beforeExecute(t, r);
//                if (dynamicExtraLoadFactor.couldStep() && getQueue().isEmpty() && wm.isNotPartitionedOrDrained()) {
//                    boolean increased = dynamicExtraLoadFactor.maybeIncrease();
//                    if (increased) {
//                        log.warn("No work to do! Increased dynamic load factor to {}", dynamicExtraLoadFactor.getCurrent());
//                    }
//                }
////                if (getQueue().size() < 100 && wm.isNotPartitionedOrDrained()) {
////                    log.warn("Less than 100 tasks left!");
////                }
//            }
//        };

        this.wm = new WorkManager<>(newOptions, consumer);

        ConsumerManager<K, V> consumerMgr = new ConsumerManager<>(consumer);

        this.brokerPollSubsystem = new BrokerPollSystem<>(consumerMgr, wm, this, newOptions);

        rb = new RingBufferManager<>(options, wm, this, workerPool);

        if (options.isProducerSupplied()) {
            this.producerManager = Optional.of(new ProducerManager<>(options.getProducer(), consumerMgr, this.wm, options));
            if (options.isUsingTransactionalProducer())
                this.committer = this.producerManager.get();
            else
                this.committer = this.brokerPollSubsystem;
        } else {
            this.producerManager = Optional.empty();
            this.committer = this.brokerPollSubsystem;
        }
    }

    private void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumerToCheck) {
        if (consumerToCheck instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumerToCheck.subscription();
        Set<TopicPartition> assignment = consumerToCheck.assignment();
        if (!subscription.isEmpty() || !assignment.isEmpty()) {
            throw new IllegalStateException("Consumer subscription must be managed by this class. Use " + this.getClass().getName() + "#subcribe methods instead.");
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
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();
        try {
            log.debug("Partitions revoked (onPartitionsRevoked), state: {}", state);
            commitOffsetsThatAreReady();
            wm.onPartitionsRevoked(partitions);
            usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsRevoked(partitions));
        } catch (Exception e) {
            throw new InternalRuntimeError("onPartitionsRevoked event error", e);
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
        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions));
        log.info("Assigned {} partitions - that's {} bytes per partition for encoding offset overruns", numberOfAssignedPartitions, OffsetMapCodecManager.DefaultMaxMetadataSize / numberOfAssignedPartitions);
    }

    @Getter
    private int numberOfAssignedPartitions;

    /**
     * Delegate to {@link WorkManager}
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
     * this is more reliable in a correctness sense, but britle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    @SneakyThrows
    private void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        if (consumer instanceof KafkaConsumer) {
            // Commons lang FieldUtils#readField - avoid needing commons lang
            Field coordinatorField = consumer.getClass().getDeclaredField("coordinator"); //NoSuchFieldException
            coordinatorField.setAccessible(true);
            ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

            Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
            autoCommitEnabledField.setAccessible(true);
            Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

            if (isAutoCommitEnabled)
                throw new IllegalStateException("Consumer auto commit must be disabled, as commits are handled by the library.");
        } else {
            // noop - probably MockConsumer being used in testing - which doesn't do auto commits
        }
    }

    @Override
    public void poll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction) {
        Function<ConsumerRecord<K, V>, List<Object>> wrappedUserFunc = (record) -> {
            log.trace("asyncPoll - Consumed a record ({}), executing void function...", record.offset());

            carefullyRun(usersVoidConsumptionFunction, record);

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                   Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        // todo refactor out the producer system to a sub class
        if (!options.isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function<ConsumerRecord<K, V>, List<ConsumeProduceResult<K, V, K, V>>> wrappedUserFunc = (consumedRecord) -> {

            List<ProducerRecord<K, V>> recordListToProduce = carefullyRun(userFunction, consumedRecord);

            if (recordListToProduce.isEmpty()) {
                log.warn("No result returned from function to send.");
            }
            log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", consumedRecord, recordListToProduce);

            List<ConsumeProduceResult<K, V, K, V>> results = new ArrayList<>();
            for (ProducerRecord<K, V> toProduce : recordListToProduce) {
                RecordMetadata produceResultMeta = producerManager.get().produceMessage(toProduce);
                var result = new ConsumeProduceResult<>(consumedRecord, toProduce, produceResultMeta);
                results.add(result);
            }
            return results;
        };

        supervisorLoop(wrappedUserFunc, callback);
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        pollAndProduceMany(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction) {
        pollAndProduce(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction,
                               Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        pollAndProduceMany((record) -> UniLists.of(userFunction.apply(record)), callback);
    }

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
    public void close(Duration timeout, DrainingMode drainMode) {
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
                    log.info("Not waiting for in flight to complete, will transition directly to closing");
                    transitionToClosing();
                }
            }

            waitForClose(timeout);
        }

        if (controlThreadFuture.isPresent()) {
            log.debug("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(toSeconds(timeout), SECONDS); // throws exception if supervisor saw one
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
                        "(state was {}). Try increasing your time-out to allow the system to drain, or close withing " +
                        "draining.", state, e);
                throw e;
            }
            log.trace("Still waiting for system to close...");
        }
    }

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException {
        log.debug("Doing closing state: {}...", state);

        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        maybeCloseConsumer();

        producerManager.ifPresent(x -> x.close(timeout));

        log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done count: {}", unfinished.size());
        }

        log.trace("Awaiting worker pool termination...");
        boolean interrupted = true;
        while (interrupted) {
            log.debug("Still interrupted");
            try {
                boolean terminationFinishedWithoutTimeout = workerPool.awaitTermination(toSeconds(DrainingCloseable.DEFAULT_TIMEOUT), SECONDS);
                interrupted = false;
                if (!terminationFinishedWithoutTimeout) {
                    log.warn("Thread execution pool termination await timeout! Were any processing jobs dead locked or otherwise stuck?");
                    boolean shutdown = workerPool.isShutdown();
                    boolean terminated = workerPool.isTerminated();
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                interrupted = true;
            }
        }

        log.debug("Close complete.");
        this.state = closed;
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

    /**
     * Block the calling thread until no more messages are being processed.
     */
    @SneakyThrows
    public void waitForProcessedNotCommitted(Duration timeout) {
        log.debug("Waiting processed but not committed...");
        var timer = Time.SYSTEM.timer(timeout);
        while (wm.isRecordsAwaitingToBeCommitted()) {
            log.trace("Waiting for no in processing...");
            Thread.sleep(100);
            timer.update();
            if (timer.isExpired()) {
                throw new TimeoutException("Waiting for no more records in processing");
            }
        }
        log.debug("No longer anything in flight.");
    }

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = wm.isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    private void transitionToDraining() {
        log.debug("Transitioning to draining...");
        this.state = State.draining;
        interruptControlThread();
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to make tests
     * run faster, or to move on to shutting down the {@link BrokerPollSystem} so that less messages are downloaded and
     * queued.
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
     * Supervisor loop for the main loop.
     *
     * @see #supervisorLoop(Function, Consumer)
     */
    protected <R> void supervisorLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                      Consumer<R> callback) {
        log.info("Control loop starting up...");

        if (state != State.unused) {
            throw new IllegalStateException(msg("Invalid state - the consumer cannot be used more than once (current " +
                    "state is {})", state));
        } else {
            state = running;
        }

        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            Thread controlThread = Thread.currentThread();
            controlThread.setName("control");
            log.trace("Control task scheduled");
            this.blockableControlThread = controlThread;
            while (state != closed) {
                try {
                    controlLoop(userFunction, callback);
                } catch (Exception e) {
                    log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
                    throw new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                }
            }
            log.info("Control loop ending clean (state:{})...", state);
            return true;
        };


//        startRingBuffer(ringbufferRunnables, userFunction, callback);

        brokerPollSubsystem.start();


        rb.startRingBuffer(userFunction, callback);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    /**
     * Main control loop
     */
    private <R> void controlLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                 Consumer<R> callback) throws TimeoutException, ExecutionException, InterruptedException {
//        handleWork(userFunction, callback);


        if (state == running) {
            if (!wm.isSufficientlyLoaded()) {
                log.debug("Found not enough messages queued up, ensuring poller is awake");
                brokerPollSubsystem.wakeup();
            } else {
                log.info("");
            }
        }

//        log.trace("Loop: Process mailbox");
//        processWorkCompleteMailBox();

        if (state == running) {
            // offsets will be committed when the consumer has its partitions revoked
            log.trace("Loop: Maybe commit");
            commitOffsetsMaybe();
        }

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        log.trace("Current state: {}", state);
        switch (state) {
            case draining -> {
                drain();
            }
            case closing -> {
                doClose(DrainingCloseable.DEFAULT_TIMEOUT);
            }
        }

        // sanity - supervise the poller
        brokerPollSubsystem.supervise();

        Duration duration = Duration.ofMillis(1);
//        log.debug("Thread yield {}", duration);
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            log.debug("Woke up", e);
        }

        // end of loop
        log.trace("End of control loop, waiting processing {}, remaining in partition queues: {}, out for processing: {}. In state: {}",
                wm.getTotalWorkWaitingProcessing(), wm.getNumberOfEntriesInPartitionQueues(), wm.getRecordsOutForProcessing(), state);
    }

    RateLimiter rateLimiter = new RateLimiter();

    private <R> void handleWork(final Function<ConsumerRecord<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        if (state == running || state == draining) {
            int dynamicExtraLoadFactorCurrent = dynamicExtraLoadFactor.getCurrent();
            int target = getPoolQueueTarget() * dynamicExtraLoadFactorCurrent;
            BlockingQueue<Runnable> queue = workerPool.getQueue();
            int current = queue.size();
            int delta = target - current;

            log.debug("Loop: Get work - target: {} current queue size: {}, requesting: {}, loading factor: {}", target, current, delta, dynamicExtraLoadFactorCurrent);
            var records = wm.<R>maybeGetWork(delta);

            log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }

//        log.debug("Pool stats: {}", workerPool);

        if (isPoolQueueLow() && dynamicExtraLoadFactor.isWarmUpPeriodOver()) {
            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();
            if (steppedUp) {
                log.warn("Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        workerPool.getQueue().size(), getPoolQueueTarget(), dynamicExtraLoadFactor.getCurrent());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                log.warn("Max loading factor steps reached: {}/{}", dynamicExtraLoadFactor.getCurrent(), dynamicExtraLoadFactor.getMax());
            }
        }

        rateLimiter.limit(() -> {
            int queueSize = workerPool.getQueue().size();
            log.info("Stats: \n- pool active: {} queued:{} \n- queue size: {} target: {} loading factor: {}",
                    workerPool.getActiveCount(), queueSize, queueSize, getPoolQueueTarget(), dynamicExtraLoadFactor.getCurrent());
        });
//        else if (dynamicExtraLoadFactor.isWarmUpPeriodOver()) {
//            log.warn("Executor pool queue is OK! {} vs {}",
//                    workerPool.getQueue().size(), getPoolQueueTarget());
//        }
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolQueueTarget() {
//        int loadingFactor = options.getLoadingFactor();
//        return options.getNumberOfThreads() * loadingFactor;
        return options.getNumberOfThreads();
    }

    private boolean isPoolQueueLow() {
        double ninteyPercent = 0.9; // at least 90% of threads are utilised
        boolean threadsUnderUtilised = workerPool.getActiveCount() < (options.getNumberOfThreads() * ninteyPercent);
        return getPoolQueueTarget() > workerPool.getQueue().size()
                && wm.isNotPartitionedOrDrained()
                && threadsUnderUtilised;
    }

    private void drain() {
        log.debug("Signaling to drain...");
        brokerPollSubsystem.drain();
        if (!isRecordsAwaitingProcessing()) {
            transitionToClosing();
        }
    }

    private void transitionToClosing() {
        log.debug("Transitioning to closing...");
        if (state == State.unused) {
            state = closed;
        } else {
            state = State.closing;
        }
        interruptControlThread();
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing.
     */
    private void processWorkCompleteMailBox() {
        log.trace("Processing mailbox (might block waiting for results)...");
        Set<WorkContainer<K, V>> results = new HashSet<>();
        final Duration timeout = getTimeToNextCommit(); // don't sleep longer than when we're expected to maybe commit

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // blocking get the head of the queue
        WorkContainer<K, V> firstBlockingPoll = null;
        try {
            if (workMailBox.isEmpty()) {
                log.debug("Blocking poll on work until next scheduled offset commit attempt for {}", timeout);
                currentlyPollingWorkCompleteMailBox.getAndSet(true);
                // wait for work, with a timeout for sanity
                firstBlockingPoll = workMailBox.poll(timeout.toMillis(), MILLISECONDS);
                currentlyPollingWorkCompleteMailBox.getAndSet(false);
            } else {
                // don't set the lock or log anything
                firstBlockingPoll = workMailBox.poll();
            }
        } catch (InterruptedException e) {
            log.debug("Interrupted waiting on work results");
        }
        if (firstBlockingPoll == null) {
            log.debug("Mailbox results returned null, indicating timeout (which was set as {}) or interruption during a blocking wait for returned work results", timeout);
        } else {
            results.add(firstBlockingPoll);
        }

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = workMailBox.size();
        log.trace("Draining {} more, got {} already...", size, results.size());
        workMailBox.drainTo(results, size);
//        for (var ignore : range(size)) {
//            // #drainTo is nondeterministic during concurrent access - poll is more deterministic and we limit our loops to ensure progress, at the cost of some performance
//            WorkContainer<K, V> secondPollNonBlocking = null; // if we poll too many, don't block
//            try {
//                secondPollNonBlocking = workMailBox.poll(0, SECONDS);
//            } catch (InterruptedException e) {
//                log.debug("Interrupted waiting on work results", e);
//            }
//            if (secondPollNonBlocking != null) {
//                results.add(secondPollNonBlocking);
//            }
//        }

        log.trace("Processing drained work {}...", results.size());
        for (var work : results) {
            MDC.put("offset", work.toString());
            handleFutureResult(work);
            MDC.clear();
        }
    }

    /**
     * Conditionally commit offsets to broker
     */
    private void commitOffsetsMaybe() {
        Duration elapsedSinceLast = getTimeSinceLastCommit();
        boolean commitFrequencyOK = toSeconds(elapsedSinceLast) >= toSeconds(timeBetweenCommits);
        boolean poolQueueLow = isPoolQueueLow();
        boolean shouldCommitNow = commitFrequencyOK || !lingeringOnCommitWouldBeBeneficial() || isCommandedToCommit() || poolQueueLow;
        if (shouldCommitNow) {
            if (!commitFrequencyOK) {
                log.debug("Commit too frequent, but no benefit in lingering");
            }
            if (poolQueueLow)
                // todo got to change this - commits are ever few ms
                log.debug("Pool queue too low so committing offsets");
            commitOffsetsThatAreReady();
            lastCommit = Instant.now();
        } else {
            if (log.isDebugEnabled()) {
                if (wm.hasCommittableOffsets()) {
                    log.debug("Have offsets to commit, but not enough time elapsed ({}), waiting for at least {}...", elapsedSinceLast, timeBetweenCommits);
                } else {
                    log.trace("Could commit now, but no offsets commitable");
                }
            }
        }
    }

    /**
     * Under some conditions, waiting longer before committing can be faster
     *
     * @return
     */
    private boolean lingeringOnCommitWouldBeBeneficial() {
        // work is waiting to be done
        boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeCompletedSuccessfully();
        // no work is currently being done
        boolean noWorkInFlight = !wm.hasWorkInFlight();
        // work mailbox is empty
        boolean workWaitingInMailbox = !workMailBox.isEmpty();
        log.trace("workIsWaitingToBeCompletedSuccessfully {} || noWorkInFlight {} || workWaitingInMailbox {};", workIsWaitingToBeCompletedSuccessfully, noWorkInFlight, workWaitingInMailbox);
        return workIsWaitingToBeCompletedSuccessfully || noWorkInFlight || workWaitingInMailbox;
    }

    private Duration getTimeToNextCommit() {
        // draining is a normal running mode for the controller
        if (state == running || state == draining) {
            return getTimeBetweenCommits().minus(getTimeSinceLastCommit());
        } else {
            log.debug("System not {} (state: {}), so don't wait to commit, only a small thread yield time", running, state);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCommit() {
        Instant now = clock.getNow();
        return Duration.between(lastCommit, now);
    }

    private void commitOffsetsThatAreReady() {
        if (wm.isClean()) {
            log.debug("Nothing changed since last commit, skipping");
            return;
        }
        committer.retrieveOffsetsAndCommit();
    }

    protected void handleFutureResult(WorkContainer<K, V> wc) {
        if (wc.getUserFunctionSucceeded().get()) {
            onSuccess(wc);
        } else {
            onFailure(wc);
        }
    }

    private void onFailure(WorkContainer<K, V> wc) {
        // error occurred, put it back in the queue if it can be retried
        // if not explicitly retriable, put it back in with an try counter so it can be later given up on
        wm.failed(wc);
    }

    protected void onSuccess(WorkContainer<K, V> wc) {
        log.trace("Processing success...");
        wm.success(wc);
    }

    /**
     * Submit a piece of work to the processing pool.
     *
     * @param workToProcess the polled records to process
     */
    private <R> void submitWorkToPool(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                      Consumer<R> callback,
                                      List<WorkContainer<K, V>> workToProcess) {
        if (!workToProcess.isEmpty()) {
            log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), workerPool);
            for (var work : workToProcess) {
                // for each record, construct dispatch to the executor and capture a Future
                log.trace("Sending work ({}) to pool", work);
                Future outputRecordFuture = workerPool.submit(() -> {
                    return userFunctionRunner(usersFunction, callback, work);
                });
                work.setFuture(outputRecordFuture);
            }
        }
    }

    /**
     * Run the supplied function.
     */
    protected <R> List<Tuple<ConsumerRecord<K, V>, R>> userFunctionRunner(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                                                          Consumer<R> callback,
                                                                          WorkContainer<K, V> wc) {
        // call the user's function
        List<R> resultsFromUserFunction;
        try {
            MDC.put("offset", wc.toString());
            log.trace("Pool received: {}", wc);

            ConsumerRecord<K, V> rec = wc.getCr();
            resultsFromUserFunction = usersFunction.apply(rec);

            onUserFunctionSuccess(wc, resultsFromUserFunction);

            // capture each result, against the input record
            var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
            for (R result : resultsFromUserFunction) {
                log.trace("Running users's call back...");
                callback.accept(result);
            }
            log.trace("User function future registered");
            // fail or succeed, either way we're done
            addToMailBoxOnUserFunctionSuccess(wc, resultsFromUserFunction);
            return intermediateResults;
        } catch (Exception e) {
            // handle fail
            log.debug("Error processing record", e);
            wc.onUserFunctionFailure();
            addToMailbox(wc); // always add on error
            throw e; // trow again to make the future failed
        }
//        finally {
//            onWorkFunctionFinish();
//        }
    }

//    private void onWorkFunctionFinish() {
//        checkEnoughWorkIsQueued()
//    }
//
//    private void checkEnoughWorkIsQueued() {
//        if (isPoolQueueLow()) {
//            notifyNewWorkRegistered();
//        }
//    }

    protected void addToMailBoxOnUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        addToMailbox(wc);
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    protected void addToMailbox(WorkContainer<K, V> wc) {
        log.trace("Adding {} to mailbox...", wc);
        workMailBox.add(wc);
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while polling the mail box.
     *
     * @see #processWorkCompleteMailBox
     * @see #blockableControlThread
     */
    void notifyNewWorkRegistered() {
        if (currentlyPollingWorkCompleteMailBox.get()) {
            if (producerManager.isPresent() && !producerManager.get().isTransactionInProgress()) {
                log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
                this.blockableControlThread.interrupt();
            } else {
                log.trace("Would have interrupted control thread, but TX in progress");
            }
        } else {
            log.trace("Work box not being polled currently, so thread not blocked, will come around to the bail box in the next looop.");
        }
    }

    @Override
    public int workRemaining() {
        return wm.getNumberOfEntriesInPartitionQueues();
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     */
    void addLoopEndCallBack(Runnable r) {
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
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            boolean commitAsap = this.commitCommand.get();
            if (commitAsap) {
                log.debug("Command to commit asap received, clearing");
                this.commitCommand.set(false);
            }
            return commitAsap;
        }
    }

}

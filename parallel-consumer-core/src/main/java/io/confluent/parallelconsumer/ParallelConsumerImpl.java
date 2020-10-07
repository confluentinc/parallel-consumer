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
import org.apache.kafka.common.KafkaException;
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

import static io.confluent.parallelconsumer.ParallelConsumerImpl.State.*;
import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.csid.utils.StringUtils.msg;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @see ParallelConsumer
 */
@Slf4j
public class ParallelConsumerImpl<K, V> implements ParallelConsumeThenProduce<K, V>, ConsumerRebalanceListener, Closeable {

    protected static final Duration defaultTimeout = Duration.ofSeconds(10); // can increase if debugging

    /**
     * Injectable clock for testing
     */
    @Setter(AccessLevel.PACKAGE)
    private WallClock clock = new WallClock();

    @Setter
    @Getter
    private Duration timeBetweenCommits = ofSeconds(1);

    private Instant lastCommit = Instant.now();

    private boolean inTransaction = false;

    private final org.apache.kafka.clients.producer.Producer<K, V> producer;
    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users's supplied function
     */
    final private ExecutorService workerPool;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    @Getter(AccessLevel.PACKAGE)
    protected WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    private final BlockingQueue<WorkContainer<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    /**
     * Useful for testing async code
     */
    final private List<Runnable> controlLoopHooks = new ArrayList<>();

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

    /**
     * The run state of the controller.
     *
     * @see #state
     */
    enum State {
        unused, running, draining, closing, closed;
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

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    public ParallelConsumerImpl(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                org.apache.kafka.clients.producer.Producer<K, V> producer,
                                ParallelConsumerOptions options) {
        log.debug("Confluent async consumer initialise");

        Objects.requireNonNull(consumer);
        Objects.requireNonNull(producer);
        Objects.requireNonNull(options);

        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);

        //
        this.producer = producer;
        this.consumer = consumer;

        workerPool = Executors.newFixedThreadPool(options.getNumberOfThreads());

        //
        this.wm = new WorkManager<>(options, consumer);

        //
        this.brokerPollSubsystem = new BrokerPollSystem<>(consumer, wm, this);

        //
        try {
            log.debug("Initialising producer transaction session...");
            producer.initTransactions();
        } catch (KafkaException e) {
            log.error("Make sure your producer is setup for transactions - specifically make sure it's {} is set.", ProducerConfig.TRANSACTIONAL_ID_CONFIG, e);
            throw e;
        }
    }

    private void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        if (consumer instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumer.subscription();
        Set<TopicPartition> assignment = consumer.assignment();
        if (subscription.size() != 0 || assignment.size() != 0) {
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
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        commitOffsetsThatAreReady();
        wm.onPartitionsRevoked(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsRevoked(partitions));
    }

    /**
     * Delegate to {@link WorkManager}
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions));
    }

    /**
     * Delegate to {@link WorkManager}
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
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
            usersVoidConsumptionFunction.accept(record);
            return UniLists.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
                               Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        // wrap user func to add produce function
        Function<ConsumerRecord<K, V>, List<ConsumeProduceResult<K, V, K, V>>> wrappedUserFunc = (consumedRecord) -> {
            List<ProducerRecord<K, V>> recordListToProduce = userFunction.apply(consumedRecord);
            if (recordListToProduce.isEmpty()) {
                log.warn("No result returned from function to send.");
            }
            log.trace("asyncPoll and Stream - Consumed and a record ({}), and returning a derivative result record to be produced: {}", consumedRecord, recordListToProduce);

            List<ConsumeProduceResult<K, V, K, V>> results = new ArrayList<>();
            for (ProducerRecord<K, V> toProduce : recordListToProduce) {
                RecordMetadata produceResultMeta = produceMessage(toProduce);
                var result = new ConsumeProduceResult<>(consumedRecord, toProduce, produceResultMeta);
                results.add(result);
            }
            return results;
        };

        supervisorLoop(wrappedUserFunc, callback);
    }

    /**
     * Produce a message back to the broker.
     * <p>
     * Implementation uses the blocking API, performance upgrade in later versions, is not an issue for the common use
     * case ({@link #poll(Consumer)}).
     *
     * @see #pollAndProduce(Function, Consumer)
     */
    RecordMetadata produceMessage(ProducerRecord<K, V> outMsg) {
        // only needed if not using tx
        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error producing result message", exception);
                throw new RuntimeException("Error producing result message", exception);
            }
        };
        Future<RecordMetadata> send = producer.send(outMsg, callback);
        try {
            return send.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the consumer.
     * <p>
     * Uses a default timeout.
     *
     * @see #close(Duration, boolean)
     */
    @Override
    public void close() {
        // use a longer timeout, to cover fpr evey other step using the default
        Duration timeout = defaultTimeout.multipliedBy(2);
        close(timeout, true);
    }

    public void close(boolean waitForInflight) {
        this.close(defaultTimeout, waitForInflight);
    }

    /**
     * Close the consumer.
     *
     * @param timeout         how long to wait before giving up
     * @param waitForInFlight wait for messages already consumed from the broker to be processed before closing
     */
    @SneakyThrows
    public void close(Duration timeout, boolean waitForInFlight) {
        if (state == closed) {
            log.info("Already closed, checking end state..");
        } else {
            log.info("Signaling to close...");

            if (waitForInFlight) {
                log.info("Will wait for all in flight to complete before");
                transitionToDraining();
            } else {
                log.info("Not waiting for in flight to complete, will transition directly to closing");
                transitionToClosing();
            }

            waitForClose(timeout);
        }

        if (controlThreadFuture.isPresent()) {
            log.trace("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(toSeconds(timeout), SECONDS); // throws exception if supervisor saw one
        }

        log.info("Close complete.");
    }

    private void waitForClose(Duration timeout) throws TimeoutException, ExecutionException {
        log.info("Waiting on closed state...");
        while (!state.equals(closed)) {
            log.trace("Still waiting for system to close...");
            try {
                boolean signaled = this.controlThreadFuture.get().get(toSeconds(timeout), SECONDS);
                if (!signaled)
                    throw new TimeoutException("Timeout waiting for system to close (" + timeout + ")");
            } catch (InterruptedException e) {
                // ignore
                log.trace("Interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                log.error("Execution or timeout exception", e);
                throw e;
            }
        }
    }

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException {
        log.debug("Doing closing state: {}...", state);

        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        log.debug("Closing producer, assuming no more in flight...");
        if (this.inTransaction) {
            // close started after tx began, but before work was done, otherwise a tx wouldn't have been started
            producer.abortTransaction();
        }
        producer.close(timeout);

        log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done: {}", unfinished);
        }

        log.trace("Awaiting worker pool termination...");
        boolean interrupted = true;
        while (interrupted) {
            log.warn("Still interrupted");
            try {
                boolean terminationFinishedWithoutTimeout = workerPool.awaitTermination(toSeconds(defaultTimeout), SECONDS);
                interrupted = false;
                if (!terminationFinishedWithoutTimeout) {
                    log.warn("workerPool await timeout!");
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
     * Block the calling thread until no more messages are being processed.
     */
    @SneakyThrows
    public void waitForNoInFlight(Duration timeout) {
        log.debug("Waiting for no in flight...");
        var timer = Time.SYSTEM.timer(timeout);
        while (!noInFlight()) {
            log.trace("Waiting for no in flight...");
            Thread.sleep(100);
            timer.update();
            if (timer.isExpired()) {
                throw new TimeoutException("Waiting for no more records in-flight");
            }
        }
        log.debug("No longer anything in flight.");
    }

    private boolean noInFlight() {
        return !wm.isWorkRemaining() || areMyThreadsDone();
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
        if (state != State.unused) {
            throw new IllegalStateException(msg("Invalid state - the consumer cannot be used more than once (current " +
                    "state is {})", state));
        } else {
            state = running;
        }

        //
        producer.beginTransaction();
        this.inTransaction = true;

        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            log.trace("Control task scheduled");
            Thread controlThread = Thread.currentThread();
            controlThread.setName("control");
            this.blockableControlThread = controlThread;
            while (state != closed) {
                try {
                    controlLoop(userFunction, callback);
                } catch (Exception e) {
                    log.error("Error from poll control thread ({}), will attempt controlled shutdown, then rethrow...", e.getMessage(), e);
                    doClose(defaultTimeout); // attempt to close
                    throw new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                }
            }
            log.trace("Poll task ended (state:{}).", state);
            return true;
        };

        brokerPollSubsystem.start();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    /**
     * Main control loop
     */
    private <R> void controlLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                 Consumer<R> callback) throws TimeoutException, ExecutionException {
        if (state == running) {
            log.trace("Loop: Get work");
            var records = wm.<R>maybeGetWork();

            log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }

        log.trace("Loop: Process mailbox");
        processWorkCompleteMailBox();

        log.trace("Loop: Maybe commit");
        commitOffsetsMaybe();

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        log.debug("Current state: {}", state);
        switch (state) {
            case draining -> drain();
            case closing -> doClose(defaultTimeout);
        }

        // end of loop
        log.trace("End of control loop, {} remaining in work manager. In state: {}", wm.getPartitionWorkRemainingCount(), state);
    }

    private void drain() {
        log.debug("Signaling to drain...");
        brokerPollSubsystem.drain();
        if (noInFlight()) {
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
        log.trace("Processing mailbox (might block waiting or results)...");
        Set<WorkContainer<K, V>> results = new HashSet<>();
        final Duration timeout = getTimeToNextCommit(); // don't sleep longer than when we're expected to maybe commit

        // blocking get the head of the queue
        WorkContainer<K, V> firstBlockingPoll = null;
        try {
            log.debug("Blocking until next scheduled offset commit attempt for {}", timeout);
            currentlyPollingWorkCompleteMailBox.getAndSet(true);
            // wait for work, with a timeout for sanity
            firstBlockingPoll = workMailBox.poll(timeout.toMillis(), MILLISECONDS);
            currentlyPollingWorkCompleteMailBox.getAndSet(false);
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
        for (var ignore : range(size)) {
            // #drainTo is nondeterministic during concurrent access - poll is more deterministic and we limit our loops to ensure progress, at the cost of some performance
            WorkContainer<K, V> secondPollNonBlocking = null; // if we poll too many, don't block
            try {
                secondPollNonBlocking = workMailBox.poll(0, SECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted waiting on work results", e);
            }
            if (secondPollNonBlocking != null) {
                results.add(secondPollNonBlocking);
            }
        }

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
        if (commitFrequencyOK || lingeringOnCommitWouldBeBeneficial()) {
            if (!commitFrequencyOK) {
                log.trace("Commit too frequent, but no benefit in lingering");
            }
            commitOffsetsThatAreReady();
            lastCommit = Instant.now();
        } else {
            if (wm.hasComittableOffsets()) {
                log.debug("Have offsets to commit, but not enough time elapsed ({}), waiting for at least {}...", elapsedSinceLast, timeBetweenCommits);
            } else {
                log.trace("Could commit now, but no offsets commitable");
            }
        }
    }

    /**
     * Under some conditions, waiting longer before committing can be faster
     *
     * @return
     */
    private boolean lingeringOnCommitWouldBeBeneficial() {
        // no work is waiting to be done
        boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeCompletedSuccessfully();
        // no work is currently being done
        boolean noWorkInFlight = wm.hasWorkInFlight();
        // work mailbox is empty
        boolean workMailBoxEmpty = workMailBox.isEmpty();

        return workIsWaitingToBeCompletedSuccessfully || noWorkInFlight || workMailBoxEmpty;
    }

    private Duration getTimeToNextCommit() {
        if (state == running) {
            return getTimeBetweenCommits().minus(getTimeSinceLastCommit());
        } else {
            log.debug("System not {}, so don't wait to commit", running);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCommit() {
        Instant now = clock.getNow();
        return Duration.between(lastCommit, now);
    }

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    private void commitOffsetsThatAreReady() {
        log.trace("Loop: Find completed work to commit offsets");
        // todo shouldn't be removed until commit succeeds (there's no harm in committing the same offset twice)
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.findCompletedEligibleOffsetsAndRemove();
        if (offsetsToSend.isEmpty()) {
            log.trace("No offsets ready");
        } else {
            log.debug("Committing offsets for {} partition(s): {}", offsetsToSend.size(), offsetsToSend);
            ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

            producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
            // see {@link KafkaProducer#commit} this can be interrupted and is safe to retry
            boolean notCommitted = true;
            int retryCount = 0;
            int arbitrarilyChosenLimitForArbitraryErrorSituation = 200;
            Exception lastErrorSavedForRethrow = null;
            while (notCommitted) {
                if (retryCount > arbitrarilyChosenLimitForArbitraryErrorSituation) {
                    String msg = msg("Retired too many times ({} > limit of {}), giving up. See error above.", retryCount, arbitrarilyChosenLimitForArbitraryErrorSituation);
                    log.error(msg, lastErrorSavedForRethrow);
                    throw new RuntimeException(msg, lastErrorSavedForRethrow);
                }
                try {
                    if (producer instanceof MockProducer) {
                        // see bug https://issues.apache.org/jira/browse/KAFKA-10382
                        // KAFKA-10382 - MockProducer is not ThreadSafe, ideally it should be as the implementation it mocks is
                        synchronized (producer) {
                            producer.commitTransaction();
                        }
                    } else {
                        producer.commitTransaction();
                    }

                    this.inTransaction = false;

                    wm.onOffsetCommitSuccess(offsetsToSend);

                    notCommitted = false;
                    if (retryCount > 0) {
                        log.warn("Commit success, but took {} tries.", retryCount);
                    }
                } catch (Exception e) {
                    log.warn("Commit exception, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                    lastErrorSavedForRethrow = e;
                    retryCount++;
                }
            }

            // begin tx for next cycle
            producer.beginTransaction();
            this.inTransaction = true;
        }
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

        for (var work : workToProcess) {
            // for each record, construct dispatch to the executor and capture a Future
            log.trace("Sending work ({}) to pool", work);
            Future outputRecordFuture = workerPool.submit(() -> {
                return userFunctionRunner(usersFunction, callback, work);
            });
            work.setFuture(outputRecordFuture);
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
            log.debug("Error in user function", e);
            wc.onUserFunctionFailure();
            addToMailbox(wc); // always add on error
            throw e; // trow again to make the future failed
        }
    }


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
            log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
            this.blockableControlThread.interrupt();
        } else {
            log.trace("Work box not being polled currently, so thread not blocked, will come around to the bail box in the next looop.");
        }
    }

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    public int workRemaining() {
        return wm.getPartitionWorkRemainingCount();
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

}

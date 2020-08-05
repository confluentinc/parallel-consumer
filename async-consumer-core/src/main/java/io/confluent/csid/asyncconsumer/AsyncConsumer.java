package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.WallClock;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.MDC;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.asyncconsumer.AsyncConsumer.State.closed;
import static io.confluent.csid.asyncconsumer.AsyncConsumer.State.running;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.csid.utils.StringUtils.msg;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Asynchronous / concurrent message consumer for Kafka.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see #asyncPoll(Consumer)
 * @see #asyncPollAndProduce(Function, Consumer)
 */
@Slf4j
public class AsyncConsumer<K, V> implements Closeable {

    protected static final Duration defaultTimeout = Duration.ofSeconds(10); // can increase if debugging

    /**
     * Injectable clock for testing
     */
    @Setter
    private WallClock clock = new WallClock();

    @Setter
    @Getter
    private Duration timeBetweenCommits = ofSeconds(1);

    private Instant lastCommit = Instant.now();

    private final org.apache.kafka.clients.producer.Producer<K, V> producer;
    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users's supplied function
     */
    final private ExecutorService workerPool;

    private Optional<Future<?>> controlThreadFuture = Optional.empty();

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
     * @see #processWorkMailBox
     */
    private Thread blockableControlThread;

    /**
     * @see #notifyNewWorkRegistered
     * @see #processWorkMailBox
     */
    private final AtomicBoolean pollingWorkMailBox = new AtomicBoolean();

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
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see AsyncConsumerOptions
     */
    public AsyncConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                         org.apache.kafka.clients.producer.Producer<K, V> producer,
                         AsyncConsumerOptions options) {
        log.info("Confluent async consumer initialise");

        Objects.requireNonNull(consumer);
        Objects.requireNonNull(producer);
        Objects.requireNonNull(options);

        //
        this.producer = producer;
        this.consumer = consumer;

        workerPool = Executors.newFixedThreadPool(options.getNumberOfThreads());

        //
        this.wm = new WorkManager<>(options);

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

    /**
     * Register a function to be applied in parallel to each received message
     *
     * @param usersVoidConsumptionFunction the function
     */
    public void asyncPoll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction) {
        Function<ConsumerRecord<K, V>, List<Object>> wrappedUserFunc = (record) -> {
            log.trace("asyncPoll - Consumed a record ({}), executing void function...", record.offset());
            usersVoidConsumptionFunction.accept(record);
            return List.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns a {@link
     * ProducerRecord} to be sent back to the broker.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    @SneakyThrows
    public void asyncPollAndProduce(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
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
     * case ({@link #asyncPoll(Consumer)}).
     *
     * @see #asyncPollAndProduce(Function, Consumer)
     */
    RecordMetadata produceMessage(ProducerRecord<K, V> outMsg) {
        Future<RecordMetadata> send = producer.send(outMsg);
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
                transitionToDraining();
            } else {
                transitionToClosing();
            }

            waitForClose(timeout);
        }

        if (controlThreadFuture.isPresent()) {
            log.trace("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(timeout.toSeconds(), SECONDS); // throws exception if supervisor saw one
        }

        log.info("Close complete.");
    }

    @SneakyThrows
    private void waitForClose(Duration timeout) {
        log.info("Waiting on closed state...");
        waitAtMost(timeout).pollInterval(ofMillis(10)).until(() -> closed.equals(state));
    }

    @SneakyThrows
    private void doClose(Duration timeout) {
        log.debug("Doing closing state: {}...", state);

        log.debug("Closing producer, assuming no more in flight...");
        producer.close(timeout);

        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done: {}", unfinished);
        }
        log.trace("Awaiting worker pool termination...");
        boolean terminationFinished = workerPool.awaitTermination(defaultTimeout.toSeconds(), SECONDS);
        if (!terminationFinished) {
            log.warn("workerPool termination interrupted!");
            boolean shutdown = workerPool.isShutdown();
            boolean terminated = workerPool.isTerminated();
        }

        log.debug("Close complete.");

        this.state = closed;
    }

    /**
     * Block the calling thread until no more messages are being processed.
     */
    public void waitForNoInFlight(Duration timeout) {
        log.debug("Waiting for no in flight...");
        waitAtMost(timeout).alias("Waiting for no more records in-flight").until(this::noInFlight);
        log.debug("No longer anything in flight.");
    }

    private boolean noInFlight() {
        return !wm.isWorkReamining() || areMyThreadsDone();
    }

    private void transitionToDraining() {
        log.debug("Transitioning to draining...");
        this.state = State.draining;
    }

    private boolean areMyThreadsDone() {
        if (controlThreadFuture.isEmpty()) {
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
            throw new IllegalStateException(msg("Invalid state - must be {}", State.unused));
        } else {
            state = running;
        }

        //
        producer.beginTransaction();

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
                                 Consumer<R> callback) {
        log.trace("Loop: Get work");
        var records = wm.<R>maybeGetWork();

        log.trace("Loop: Submit to pool");
        submitWorkToPool(userFunction, callback, records);

        log.trace("Loop: Process mailbox");
        processWorkMailBox();

        log.trace("Loop: Maybe commit");
        commitOffsetsMaybe();

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        switch (state) {
            case draining -> drain();
            case closing -> doClose(defaultTimeout);
        }

        // end of loop
        log.trace("End of control loop, {} remaining in work manager. In state: {}", wm.getPartitionWorkRemainingCount(), state);
        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            log.warn("Awoken", e);
        }
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
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing.
     */
    private void processWorkMailBox() {
        log.trace("Processing mailbox (might block waiting or results)...");
        Set<WorkContainer<K, V>> results = new HashSet<>();
        final Duration timeout = getTimeToNextCommit(); // don't sleep longer than when we're expected to maybe commit

        // blocking get the head of the queue
        WorkContainer<K, V> firstBlockingPoll = null;
        try {
            log.debug("Blocking until next scheduled offset commit attempt for {}", timeout);
            pollingWorkMailBox.getAndSet(true);
            // wait for work, with a timeout for sanity
            firstBlockingPoll = workMailBox.poll(timeout.toMillis(), MILLISECONDS);
            pollingWorkMailBox.getAndSet(false);
        } catch (InterruptedException e) {
            log.trace("Interrupted waiting on work results");
        }
        if (firstBlockingPoll == null) {
            log.debug("Mailbox results returned null, indicating timeout (after {}) during a blocking wait for returned work results", timeout);
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
        boolean commitFrequencyOK = elapsedSinceLast.toSeconds() >= timeBetweenCommits.toSeconds();
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
            log.debug("System not running as normal, don't wait to commit");
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
            log.trace("Committing offsets for {} partition(s): {}", offsetsToSend.size(), offsetsToSend);
            ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
            // see {@link KafkaProducer#commit} this can be interrupted and is safe to retry
            boolean notCommitted = true;
            int retryCount = 0;
            int arbitrarilyChosenLimitForArbitraryErrorSituation = 200;
            InterruptException lastErrorSavedForRethrow = null;
            while (notCommitted) {
                if (retryCount > arbitrarilyChosenLimitForArbitraryErrorSituation) {
                    String msg = msg("Retired too many times ({} > limit of {}), giving up. See error above.", retryCount, arbitrarilyChosenLimitForArbitraryErrorSituation);
                    log.error(msg, lastErrorSavedForRethrow);
                    throw new RuntimeException(msg, lastErrorSavedForRethrow);
                }
                try {
                    producer.commitTransaction();
                    notCommitted = false;
                    if (retryCount > 0) {
                        log.warn("Commit success, but took {} tries.", retryCount);
                    }
                } catch (InterruptException e) {
                    log.warn("Commit interrupted, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                    lastErrorSavedForRethrow = e;
                    retryCount++;
                }
            }

            // only open the next tx if we are running
            if (state == running) {
                producer.beginTransaction();
            }
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
     * @see #processWorkMailBox
     * @see #blockableControlThread
     */
    void notifyNewWorkRegistered() {
        if (pollingWorkMailBox.getAcquire()) {
            log.trace("Knock knock, wake up! You've got mail (tm)!");
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

    /**
     * A simple tuple structure.
     *
     * @param <L>
     * @param <R>
     */
    @Data
    public static class Tuple<L, R> {
        final private L left;
        final private R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

    /**
     * A simple triple structure to capture the set of coinciding data.
     *
     * <ul>
     *     <li>the record consumer</li>
     *     <li>any producer record produced as a result of it's procssing</li>
     *     <li>the metadata for publishing that record</li>
     * </ul>
     *
     * @param <K>  in key
     * @param <V>  in value
     * @param <KK> out key
     * @param <VV> out value
     */
    @Data
    public static class ConsumeProduceResult<K, V, KK, VV> {
        final private ConsumerRecord<K, V> in;
        final private ProducerRecord<KK, VV> out;
        final private RecordMetadata meta;
    }

}

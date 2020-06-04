package io.confluent.csid.asyncconsumer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * @param <K> key
 * @param <V> value
 */
// TODO generics for produce vs consume, different produce topics may need different generics
@Slf4j
public class AsyncConsumer<K, V> implements Closeable {

    final private org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    final private org.apache.kafka.clients.producer.Producer<K, V> producer;

    protected final Duration defaultTimeout = Duration.ofSeconds(10); // increase if debugging

    private final int numberOfThreads = 16;

    private boolean shouldPoll = true;

    // TODO configurable number of threads
    final private ExecutorService workerPool = Executors.newFixedThreadPool(numberOfThreads);

    final private ExecutorService internalPool = Executors.newSingleThreadExecutor();

    private Future<?> pollThread;

    protected WorkManager<K, V> wm;

    @Setter
    @Getter
    private Duration longPollTimeout = Duration.ofMillis(2000);

    @Setter
    @Getter
    private Duration timeBetweenCommits = ofSeconds(1);

    private Instant lastCommit = Instant.now();

    private final Queue<WorkContainer<K, V>> mailBox = new ConcurrentLinkedQueue<>(); // Thread safe, highly performant, non blocking

    private InFlightManager<K, V> inFlightManager = new InFlightManager<>();

    public AsyncConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                         org.apache.kafka.clients.producer.Producer<K, V> producer,
                         AsyncConsumerOptions asyncConsumerOptions) {
        this.consumer = consumer;
        this.producer = producer;
        wm = new WorkManager<>(100, asyncConsumerOptions);

        //
        producer.initTransactions();
    }

    /**
     * todo
     *
     * @param usersVoidConsumptionFunction
     */
    public void asyncPoll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction) {
        Function<ConsumerRecord<K, V>, List<Object>> wrappedUserFunc = (record) -> {
            log.debug("asyncPoll - Consumed a record ({}:{}), executing void function...", record.offset(), record.value());
            usersVoidConsumptionFunction.accept(record);
            List<Object> userFunctionReturnsNothing = List.of();
            return userFunctionReturnsNothing;
        };
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        asyncPollInternal(wrappedUserFunc, voidCallBack);
    }

    /**
     * todo
     *
     * @param userFunction
     * @param callback     applied after message ack'd by kafka
     * @return a stream of results for monitoring, as messages are produced into the output topic
     */
    @SneakyThrows
    public <R> void asyncPollAndProduce(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
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

        asyncPollInternal(wrappedUserFunc, callback);
    }

    RecordMetadata produceMessage(ProducerRecord<K, V> outMsg) {
        Future<RecordMetadata> send = producer.send(outMsg);
        try {
            var recordMetadata = send.get(); // TODO don't block
            return recordMetadata;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        close(defaultTimeout);
    }

    @SneakyThrows
    public void close(Duration timeout) {
        log.info("Closing...");
        waitForNoInFlight(timeout);

        shutdownPollLoop();

        List<Runnable> unfinished = workerPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            log.warn("Threads not done: {}", unfinished);
        }
        log.debug("Awaiting worker termination...");
        boolean terminationFinished = workerPool.awaitTermination(10L, SECONDS); // todo make smarter
        if (!terminationFinished) {
            log.warn("workerPool termination interrupted!");
            boolean shutdown = workerPool.isShutdown();
            boolean terminated = workerPool.isTerminated();
        }
        internalPool.shutdown();
        log.debug("Awaiting controller termination...");
        boolean internalShutdown = internalPool.awaitTermination(10L, SECONDS);

        log.debug("Closing producer and consumer...");
        this.consumer.close(defaultTimeout);
        this.producer.close(defaultTimeout);

        log.trace("Checking for control thread exception...");
        pollThread.get(timeout.toSeconds(), SECONDS); // throws exception if supervisor saw one

        log.debug("Close complete.");
    }

    public void waitForNoInFlight(Duration timeout) {
        log.debug("Waiting for no in flight...");
        waitAtMost(timeout).until(this::noInflight);
        log.debug("No longer anything in flight.");
    }

    private boolean noInflight() {
//        return !inFlightManager.hasWorkLeft() || areMyThreadsDone();
        return !wm.isWorkReamining() || areMyThreadsDone();
    }

    private void shutdownPollLoop() {
        this.shouldPoll = false;
    }


    private boolean areMyThreadsDone() {
        if (pollThread == null) {
            // not constructed yet, will become alive, unless #poll is never called // TODO fix
            return false;
        }
        return pollThread.isDone();
    }

    protected <R> void asyncPollInternal(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                         Consumer<R> callback) {
        producer.beginTransaction();

        // run main pool loop in thread
        pollThread = internalPool.submit(() -> {
            while (shouldPoll) {
                try {
                    controlLoop(userFunction, callback);
                } catch (Exception e) {
                    log.error("Error from poll control thread ({}), throwing...", e.getMessage(), e);
                    throw new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                }
            }
            log.trace("Poll loop ended.");
            return true;
        });
    }

    private <R> void controlLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                 Consumer<R> callback) {
        // TODO doesn't this forever expand if no future ever completes? eventually we'll run out of memory as we consume more and mor records without committing any
        log.trace("Loop: Poll broker");
        ConsumerRecords<K, V> polledRecords = pollBrokerForRecords();

        log.trace("Loop: Register work");
        wm.registerWork(polledRecords);

        log.trace("Loop: Get work");
        var records = wm.<R>getWork();

        log.trace("Loop: Submit to pool");
        submitWorkToPool(userFunction, callback, records);

        log.trace("Loop: Process mailbox");
        processMailBox();

        log.trace("Loop: Maybe commit");
        commitOffsetsMaybe();

        // run call back
        this.controlLoopHooks.forEach(Runnable::run);

        // end of loop
        Thread.yield();
    }

    private void processMailBox() {
        log.trace("Processing mailbox...");
        while (!mailBox.isEmpty()) {
            log.trace("Mail received...");
            var work = mailBox.poll();
            MDC.put("offset", work.toString());
            handleFutureResult(work);
            MDC.clear();
        }
    }

    // todo keep alive with broker while processing messages when too many in flight
    private ConsumerRecords<K, V> pollBrokerForRecords() {
        ConsumerRecords<K, V> records = this.consumer.poll(longPollTimeout);
        if (records.isEmpty()) {
            try {
                log.trace("No records returned, simulating long poll with sleep for {}...", longPollTimeout); // TODO remove to mock producer
                Thread.sleep(longPollTimeout.toMillis()); // todo remove - used for testing where mock consumer poll instantly (doesn't simulate waiting for new data)
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            log.debug("Polled and found {} records...", records.count());
        }
        return records;
    }

    private void commitOffsetsMaybe() {
        Instant now = Instant.now();
        Duration between = Duration.between(lastCommit, now);
        if (between.toSeconds() >= timeBetweenCommits.toSeconds()) {
            commitOffsetsThatAreReady();
            lastCommit = Instant.now();
        } else {
            if (inFlightManager.hasOffsetsToCommit()) {
                log.debug("Have offsets to commit, but not enough time elapsed ({}), waiting for at least {}...", between, timeBetweenCommits);
            }
        }
    }

    private void commitOffsetsThatAreReady() {
        log.trace("Loop: Find completed work too commit offsets");
        // todo shouldn't be removed until commit succeeds (there's no harm in comitting the same offset twice)
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.findCompletedFutureOffsets();
        if (!offsetsToSend.isEmpty()) {
            // todo retry loop? or should be made idempotent. Failure?
            log.trace("Committing offsets for {} partition(s): {}", offsetsToSend.size(), offsetsToSend);
            ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
            producer.commitTransaction();
            inFlightManager.clearOffsetsToSend();
            producer.beginTransaction();
        } else {
            log.trace("No offsets ready");
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
     * todo
     *
     * @param usersFunction
     * @param callback
     * @param workToProcess the polled records to process
     * @param <R>
     */
    private <R> void submitWorkToPool(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                      Consumer<R> callback,
                                      List<WorkContainer<K, V>> workToProcess) {

        for (var work : workToProcess) {
            // for each record, construct dispatch to the executor and capture a Future
            ConsumerRecord<K, V> cr = work.getCr();

            log.trace("Sending work ({}) to pool", work);
            Future outputRecordFuture = workerPool.submit(() -> {
                return userFunctionRunner(usersFunction, callback, work);
            });
            work.setFuture(outputRecordFuture);

            inFlightManager.addWorkToInFlight(work, cr);
        }
    }


    protected <R> List<Tuple<ConsumerRecord<K, V>, R>>
    userFunctionRunner(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
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
        wc.onUserFunctionSuccess(); // todo incorrectly succeeds vertx functions
    }

    protected void addToMailbox(WorkContainer<K, V> wc) {
        log.trace("Adding {} to mailbox...", wc);
        mailBox.add(wc);
    }

    public int workRemaining() {
        return wm.workRemainingCount();
    }

    /**
     * Useful for testing async code
     */
    final private List<Runnable> controlLoopHooks = new ArrayList<>();

    void addLoopEndCallBack(Runnable r) {
        this.controlLoopHooks.add(r);
    }

    @Data
    public static class Tuple<L, R> {
        final private L left;
        final private R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

    /**
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

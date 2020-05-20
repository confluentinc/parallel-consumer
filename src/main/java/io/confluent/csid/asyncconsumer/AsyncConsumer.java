package io.confluent.csid.asyncconsumer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * @param <K> key
 * @param <V> value
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncConsumer<K, V> implements Closeable { // TODO generics for produce vs consume, different produce topics may need different generics

    final private Consumer<K, V> consumer;
    final private Producer<K, V> producer;
    static final String INPUT_TOPIC = "input-FIXME";

    final Duration defaultTimeout = Duration.ofSeconds(20);

    private final int numberOfThreads = 3;

    private boolean shouldPoll = true;

    final private ExecutorService workerPool = Executors.newFixedThreadPool(numberOfThreads);
    final private ExecutorService internalPool = Executors.newFixedThreadPool(2);
    private Future pollThread;
    private Future supervisorThread;

    final private Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> inFlightPerPartition = new HashMap<>();

    private Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();

    private WorkManager<K, V> wm = new WorkManager<>();

    @Setter
    private Duration longPollTimeout = Duration.ofMillis(2000);

    @Setter
    private Duration timeBetweenCommits = ofSeconds(1);

    private Instant lastCommit = Instant.now();


    public void asyncVoidPoll(java.util.function.Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction) {
        this.asyncPollAndStream((record) -> {
            log.info("asyncPoll - Consumed a record ({}), executing void function...", record.value());
            usersVoidConsumptionFunction.accept(record);
            return Lists.emptyList();
        });
    }

    /**
     * @return a stream of results for monitoring, as messages are produced into the output topic
     */
    @SneakyThrows
    public Stream<Tripple<ConsumerRecord<K, V>, ProducerRecord<K, V>, RecordMetadata>> asyncPollAndProduce(
            Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {

        var tupleStream = this.asyncPollAndStream((record) -> {
            List<ProducerRecord<K, V>> apply = userFunction.apply(record);
            if (apply.isEmpty()) {
                log.warn("No result returned from function to send.");
            }
            log.info("asyncPoll and Stream - Consumed and a record ({}), and returning a derivative result record to be produced: {}", record, apply);
            return apply;
        });

        var rStream = tupleStream.map(result -> {
            ConsumerRecord<K, V> inRec = result.getLeft();
            ProducerRecord<K, V> outMsg = result.getRight();
            RecordMetadata produceResult = produceMessage(outMsg);
            return Tripple.of(inRec, outMsg, produceResult);
        });

        return rStream;
    }

    RecordMetadata produceMessage(ProducerRecord<K, V> outMsg) {
        Future<RecordMetadata> send = producer.send(outMsg);
        try {
            RecordMetadata recordMetadata = send.get(); // TODO block
            return recordMetadata;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @param <R> result type
     * @return a stream of results of applying the function to the polled records
     */
    public <R> Stream<Tuple<ConsumerRecord<K, V>, R>> asyncPollAndStream(Function<ConsumerRecord<K, V>, List<R>> userFunction) {
        ConcurrentLinkedDeque<Tuple<ConsumerRecord<K, V>, R>> results = new ConcurrentLinkedDeque<>();

        controlLoop(userFunction, results);

        Spliterator<Tuple<ConsumerRecord<K, V>, R>> spliterator = Spliterators.spliterator(new Iterator<Tuple<ConsumerRecord<K, V>, R>>() {
            @Override
            public boolean hasNext() {
                boolean notEmpty = !results.isEmpty();
                return notEmpty;
            }

            @Override
            public Tuple<ConsumerRecord<K, V>, R> next() {
                Tuple<ConsumerRecord<K, V>, R> poll = results.poll();
                return poll;
            }
        }, results.size(), Spliterator.NONNULL);

        Stream<Tuple<ConsumerRecord<K, V>, R>> stream = StreamSupport.stream(spliterator, false);

        return stream;
    }

    @Override
    public void close() {
        close(defaultTimeout);
    }

    @SneakyThrows
    public void close(Duration timeout) {
        waitForNoInflight(timeout);

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
        supervisorThread.get(); // throws exception if supervisor saw one
    }

    public void waitForNoInflight(Duration timeout) {
        log.debug("Waiting for no inflight...");
        waitAtMost(timeout).until(this::noInflight);
    }

    private boolean noInflight() {
        return !hasWorkLeft() || areMyThreadsDone();
    }

    private void shutdownPollLoop() {
        this.shouldPoll = false;
    }

    private boolean hasWorkLeft() {
        return hasInflightRemaining() || hasOffsetsToCommit();
    }

    private boolean areMyThreadsDone() {
        if (pollThread == null || supervisorThread == null) {
            // not constructed yet, will become alive, unless #poll is never called // TODO fix
            return false;
        }
        return pollThread.isDone() && supervisorThread.isDone();
    }

    private boolean hasInflightRemaining() {
        for (var entry : inFlightPerPartition.entrySet()) {
            TopicPartition x = entry.getKey();
            TreeMap<Long, WorkContainer<K, V>> y = entry.getValue();
            if (!y.isEmpty())
                return true;
        }
        return false;
    }

    private boolean hasOffsetsToCommit() {
        return !offsetsToSend.isEmpty();
    }

    //    @SneakyThrows
    private <R> void controlLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction, ConcurrentLinkedDeque<Tuple<ConsumerRecord<K, V>, R>> results) {

        producer.initTransactions();
        producer.beginTransaction();

        // run main pool loop in thread
        pollThread = internalPool.submit(() -> {
            while (shouldPoll) {
                // TODO doesn't this forever expand if no future ever completes? eventually we'll run out of memory as we consume more and mor records without committing any
                ConsumerRecords<K, V> polledRecords = pollForRecords();

                wm.registerWork(polledRecords);

                var records = wm.getWork();

                processPoll(userFunction, results, workerPool, inFlightPerPartition, records);

                // pause to allow futures to complete
                Thread.yield();

                offsetsToSend = findCompletedFutureOffsets(inFlightPerPartition);

                commitOffsetsMaybe(offsetsToSend);

                if (!shouldPoll) {
                    waitForNoInflight(defaultTimeout);
                    commitOffsetsThatAreReady(offsetsToSend);
                }
            }
        });

        // todo delete, just yse catch on outer
        // supervise the poll worker thread
        supervisorThread = internalPool.submit(() -> {
            try {
                Object o = pollThread.get();
            } catch (Exception e) {
                log.error("Error from poll control thread ({}), throwing...", e.getMessage());
                throw new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
            }
        });
    }

    // todo keep alive with broker while processing messages when too many in flight
    private ConsumerRecords<K, V> pollForRecords() {
        ConsumerRecords<K, V> records = this.consumer.poll(longPollTimeout);
        if (records.isEmpty()) {
            try {
                log.debug("No records returned, simulating long poll with sleep for {}...", longPollTimeout); // TODO remove to mock producer
                Thread.sleep(longPollTimeout.toMillis()); // todo remove - used for testing where mock consumer poll instantly (doesn't simulate waiting for new data)
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            log.debug("Polled and found {} recods...", records.count());
        }
        return records;
    }

    private void commitOffsetsMaybe(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        Instant now = Instant.now();
        Duration between = Duration.between(lastCommit, now);
        if (between.toSeconds() >= timeBetweenCommits.toSeconds()) {
            commitOffsetsThatAreReady(offsetsToSend);
            lastCommit = Instant.now();
        } else {
            if (hasOffsetsToCommit()) {
                log.debug("Have offsets to commit, but not enough time elapsed ({}), waiting for at least {}...", between, timeBetweenCommits);
            }
        }
    }

    private void commitOffsetsThatAreReady(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        if (!offsetsToSend.isEmpty()) {
            log.info("Committing offsets {}", offsetsToSend);
            ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            producer.sendOffsetsToTransaction(offsetsToSend, groupMetadata);
            producer.commitTransaction();
            this.offsetsToSend.clear();// = new HashMap<>();// todo remove? smelly?
            producer.beginTransaction();
        } else {
            log.debug("No offsets ready");
        }
    }

    @SneakyThrows
    private Map<TopicPartition, OffsetAndMetadata> findCompletedFutureOffsets(Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> inFlightPerPartition) {

        for (final var inFlightInPartition : inFlightPerPartition.entrySet()) {

            LinkedList<Long> offsetToRemove = new LinkedList<>();
            TreeMap<Long, WorkContainer<K, V>> inFlightFutures = inFlightInPartition.getValue();
            for (final var offsetAndItsWorkContainer : inFlightFutures.entrySet()) {
                // ordered iteration via offset keys thanks to the treemap
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                Future<ProducerRecord<K, V>> processingTask = container.getFuture();
                if (processingTask != null && processingTask.isDone()) {
                    try {
                        processingTask.get();
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetAndItsWorkContainer.getKey(), ""); // TODO blank string?
                        TopicPartition topicPartitionKey = inFlightInPartition.getKey();
                        // as inflights are processed in order, this will keep getting overwritten with the highest offset available
                        log.debug("Future done, adding to commit list...");
                        offsetsToSend.put(topicPartitionKey, offsetData);
                        offsetToRemove.add(offsetAndItsWorkContainer.getKey());
                        wm.success(container);
                    } catch (Exception e) {
                        // error occurred, put it back in the queue if it can be retried
                        // if not explicitly retriable, put it back in with an try counter so it can be later given up on
                        wm.failed(container);
                    }
                } else {
                    // can't commit this offset or beyond, as this is the latest offset that is incomplete
                    // i.e. only commit offsets that come before the current one, and stop looking for more
                    break;
                }
            }

            for (Long aLong : offsetToRemove) {
                inFlightFutures.remove(aLong);
            }
        }
        return offsetsToSend;
    }

    private <R> void processPoll(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                 ConcurrentLinkedDeque<Tuple<ConsumerRecord<K, V>, R>> results,
                                 ExecutorService pool,
                                 Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> inFlightPerPartition,
                                 List<WorkContainer<K, V>> records) {

        for (var work : records) {
            // for each record, construct dispatch to the executor and capture a Future
            ConsumerRecord<K, V> cr = work.getCr();
            Future outputRecordFuture = pool.submit(() -> {
                // call the user's function
                List<R> resultsFromUserFunction = usersFunction.apply(cr);
                // capture each result, against the input record
                resultsFromUserFunction.forEach(result -> {
                    Tuple<ConsumerRecord<K, V>, R> r = Tuple.of(cr, result);
                    results.add(r);
                });
                log.info("User function future complete");
                return null; // null for done - actual results are streamed
            });
            work.setFuture(outputRecordFuture);

            final TopicPartition inputTopicPartition = new TopicPartition(cr.topic(), cr.partition());

            long offset = cr.offset();
            // ensure we have a TreeMap (ordered map by key) for the topic partition we're reading from
            TreeMap<Long, WorkContainer<K, V>> offsetToFuture = inFlightPerPartition
                    .computeIfAbsent(inputTopicPartition, (ignore) -> new TreeMap<>());
            // store the future reference, against it's the offset as key
            offsetToFuture.put(offset, work);
        }
    }

    @Data
    public static class Tripple<L, M, R> {
        final private L left;
        final private M middle;
        final private R right;

        public static <LL, MM, RR> Tripple<LL, MM, RR> of(LL l, MM m, RR r) {
            return new Tripple<>(l, m, r);
        }
    }

    @Data
    public static class Tuple<L, R> {
        final private L left;
        final private R right;

        public static <LL, RR> Tuple<LL, RR> of(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

    @Data
    public static class MyResult<KEY, VALUE, RESULT> {
        final private ConsumerRecord<KEY, VALUE> input;
        final private RESULT output;
        final private RecordMetadata rm;
    }

    @Data
    public static class MyResultTwo<KEY, VALUE, RESULT> {
        final private ConsumerRecord<KEY, VALUE> input;
        final private RESULT output;
    }

}

package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

@Slf4j
public class WorkMailbox<K, V> {

    // todo make package level
    @Getter(AccessLevel.PUBLIC)
    protected final WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    @Getter(PROTECTED)
    private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking


    /**
     * @see #notifySomethingToDo
     * @see #processWorkCompleteMailBox
     */
    private final AtomicBoolean currentlyPollingWorkCompleteMailBox = new AtomicBoolean();

    public void registerWork(EpochAndRecordsMap<K, V> polledRecords) {
        log.trace("Adding {} to mailbox...", polledRecords);
        workMailBox.add(ControllerEventMessage.of(polledRecords));
    }

    protected void addToMailbox(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc) {
        String state = wc.isUserFunctionSucceeded() ? "succeeded" : "FAILED";
        log.trace("Adding {} {} to mailbox...", state, wc);
        workMailBox.add(ControllerEventMessage.of(wc));

        wc.onPostAddToMailBox(pollContext, producerManager);
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

}

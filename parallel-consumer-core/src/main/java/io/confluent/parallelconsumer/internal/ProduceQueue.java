package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerException;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

import static lombok.AccessLevel.PRIVATE;

/**
 * todo doc
 *
 * @author Antony Stubbs
 */
@ThreadSafe
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
@FieldDefaults(level = PRIVATE)
public class ProduceQueue<K, V> {

    /**
     * The number of records to not above which we don't send more to the Producer.
     * <p>
     * When there are more than this many records still buffered, waiting for ACK from the broker we, no more are sent
     * to the Producer. This causes backpressure in our work loop, which will be visible through the size of our queue.
     */
    public static final int QUEUE_THRESHOLD = 3;

    State state = State.UNUSED;

    final KafkaProducer<K, V> intProducer;

    final BlockingQueue<ProducerRecord<K, V>> outboundQueue = new LinkedBlockingQueue<>();

    final QueuedFutures accumulatingSendFutures = new QueuedFutures();

    final ExecutorService executorPool;

    Future<?> thread;
    private Exception exception;

    public ProduceQueue(KafkaProducer<K, V> producer, ExecutorService executorPool) {
        this.intProducer = producer;
        this.executorPool = executorPool;

        start();
    }

    private void start() {
        Runnable sendLoop = this::sendLoop;
        this.thread = executorPool.submit(sendLoop);
    }

    protected void supervise() {
        if (thread.isDone() && state == State.CRASHED) {
            throw new ParallelConsumerException("Producer thread has died", exception);
        }
    }

    public void send(ProducerRecord<K, V> newRecord) {
        // all records go through the queue
        enqueue(newRecord);
    }

    private void enqueue(ProducerRecord<K, V> newRecord) {
        outboundQueue.add(newRecord);
    }

    private void sendLoop() {
        state = State.RUNNING;
        while (state == State.RUNNING) {
            try {
//                blockWhileInFlightsTooHigh();
                sendOneQueuedRecordBlocking();
            } catch (InterruptedException e) {
                log.warn("Interrupted while sending message, shutting down", e);
                this.exception = e;
                state = State.CRASHED;
            }
        }
        state = State.CLOSED;
    }

    /**
     * Blocks until the broker is ready to accept more messages. As long as our in-flight count is below this, it won't
     * block.
     */
    private void blockWhileInFlightsTooHigh() {
        accumulatingSendFutures.blockUntilQueueSizeBelow(getQueueThreshold());
    }

    // todo should be dynamic, somehow, based on the broker's performance
    private double getQueueThreshold() {
        return QUEUE_THRESHOLD;
    }

    /**
     * Blocks until a record is available, then sends it.
     */
    private void sendOneQueuedRecordBlocking() throws InterruptedException {
        log.debug("Trying to get record from outbound queue to send, might block...");
        var take = outboundQueue.take();
        log.debug("Sending {} to broker", take);
        var send = intProducer.send(take);
        accumulatingSendFutures.add(send);
        log.debug("Sent record to broker, added {} to in-flight queue", send);
    }

    @ToString.Include
    public int outboundSize() {
        return accumulatingSendFutures.queueSize();
//        return outboundQueue.size();
    }

    /**
     * todo doc
     */
    private class QueuedFutures {

        private final Queue<Future<RecordMetadata>> internal = new LinkedList<>();

        public int queueSize() {
            return internal.size();
        }

        public void add(Future<RecordMetadata> send) {
            internal.add(send);
        }

        /**
         * Removes one entry at a time from the head of the FIFO queue, and waits util it's Future is completed, while
         * the queue is above the given threshold.
         */
        public void blockUntilQueueSizeBelow(double queueThreshold) {
            if (queueSize() < queueThreshold) {
                log.debug("Queue size below threshold, not blocking");
                return;
            }

            log.debug("In-flight future count {}, will try to remove completed entries to drop size below {}", queueSize(), getQueueThreshold());
            // removes entries from the queue until the queue size is below the threshold
            // but to remove an element, it has to be completed
            while (queueSize() >= queueThreshold) {
                log.debug("Queue size {} is still not below threshold {}, might block removing next head...", queueSize(), queueThreshold);
                removeHeadOfQueue();
            }
            log.debug("Finished blocking on queue size which is now {}, below threshold of {}", queueSize(), queueThreshold);
        }

        private void removeHeadOfQueue() {
            try {
                // remove the head of the queue, won't block as queue is not empty
                var take = internal.remove();
                log.debug("To remove head, waiting for future send result to complete... {}", take);
                // block until the head of the queue is complete
                take.get();
                log.debug("Send completed");
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for queue to drain, shutting down", e);
                state = State.CLOSING;
            } catch (ExecutionException e) {
                log.warn("Error sending record, shutting down", e);
                state = State.CLOSING;
            }
        }
    }
}

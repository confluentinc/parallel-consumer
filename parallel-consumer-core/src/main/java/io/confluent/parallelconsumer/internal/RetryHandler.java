package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class RetryHandler<K, V> implements Runnable {
    private BlockingQueue<AbstractParallelEoSStreamProcessor.ControllerEventMessage<K, V>> workMailBox;
    private BlockingQueue<WorkContainer<K, V>> retryQueue;

    private State state;

    public RetryHandler(PCModule<K, V> pc) {
        workMailBox = pc.pc().getWorkMailBox();
        retryQueue = pc.workManager().getSm().getRetryQueue();
        state = pc.pc().getState();
    }

    @Override
    public void run() {
        while (state != State.CLOSED) {
            if (retryCouldProcess()) {
                pollRetryQueueToMailBox();
            }
        }
    }

    private boolean retryCouldProcess() {
        WorkContainer<K, V> wc = retryQueue.peek();
        return wc != null && wc.getRetryDueAt().isBefore(Instant.now());
    }

    // poll retry queue records to mailbox queue to be processed
    // the retry queue modifications are all happening in the same thread, no need to worry about race condition
    private void pollRetryQueueToMailBox() {
        WorkContainer<K, V> wc = retryQueue.poll();
        if (wc != null) {
            log.debug("poll retry queue records to mailbox queue to be processed");
            workMailBox.add(AbstractParallelEoSStreamProcessor.ControllerEventMessage.of(wc));
        }
    }
}

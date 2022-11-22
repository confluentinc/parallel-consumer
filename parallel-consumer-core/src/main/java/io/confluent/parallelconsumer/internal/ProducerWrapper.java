package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.*;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;

import static io.confluent.parallelconsumer.internal.ProducerWrapper.ProducerState.*;

/**
 * Our extension of the standard Producer to mostly add some introspection functions and state tracking.
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class ProducerWrapper<K, V> implements Producer<K, V> {

    /**
     * Used to track Producer's transaction state, as it' isn't otherwise exposed.
     */
    public enum ProducerState {
        INSTANTIATED, INIT, BEGIN, COMMIT, ABORT, CLOSE
    }

    /**
     * Tracks the internal transaction state of the Prodocer
     */
    @ToString.Include
    @Getter
    private volatile ProducerState producerState = ProducerState.INSTANTIATED;


    @NonNull
    private final ParallelConsumerOptions<K, V> options;

    /**
     * Cached discovery of whether the underlying Producer has been set up for transactions or not.
     */
    private final boolean producerIsConfiguredForTransactions;

    // nasty reflection
    private Field txManagerField;
    private Method txManagerMethodIsCompleting;
    private Method txManagerMethodIsReady;

    @NonNull
    @Delegate(excludes = Excludes.class)
    private final Producer<K, V> producer;

    public ProducerWrapper(ParallelConsumerOptions<K, V> options) {
        this.options = options;
        producer = options.getProducer();
        this.producerIsConfiguredForTransactions = discoverIfProducerIsConfiguredForTransactions();
    }

    public boolean isMockProducer() {
        return producer instanceof MockProducer;
    }

    public boolean isConfiguredForTransactions() {
        return this.producerIsConfiguredForTransactions;
    }

    /**
     * Type erasure issue fix
     */
    interface Excludes {
        void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                      String consumerGroupId) throws ProducerFencedException;

        void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                      ConsumerGroupMetadata groupMetadata) throws ProducerFencedException;
    }

    /**
     * @deprecated use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}
     */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        log.debug("Sending offsets to transaction...");
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }


    /**
     * @return boolean which shows if we are set up for transactions or not
     */
    @SneakyThrows
    private boolean discoverIfProducerIsConfiguredForTransactions() {
        if (producer instanceof KafkaProducer) {
            txManagerField = producer.getClass().getDeclaredField("transactionManager");
            txManagerField.setAccessible(true);

            boolean producerIsConfiguredForTransactions = getProducerIsTransactional();
            if (producerIsConfiguredForTransactions) {
                TransactionManager transactionManager = getTransactionManager();
                txManagerMethodIsCompleting = transactionManager.getClass().getDeclaredMethod("isCompleting");
                txManagerMethodIsCompleting.setAccessible(true);

                txManagerMethodIsReady = transactionManager.getClass().getDeclaredMethod("isReady");
                txManagerMethodIsReady.setAccessible(true);
            }
            return producerIsConfiguredForTransactions;
        } else if (producer instanceof MockProducer) {
            // can act as both, delegate to user selection
            return options.isUsingTransactionalProducer();
        } else {
            // unknown
            return false;
        }
    }

    /**
     * Nasty reflection but better than relying on user supplying a copy of their config, maybe
     *
     * @see AbstractParallelEoSStreamProcessor#checkAutoCommitIsDisabled
     */
    @SneakyThrows
    private boolean getProducerIsTransactional() {
        if (producer instanceof MockProducer) {
            // can act as both, delegate to user selection
            return options.isUsingTransactionalProducer();
        } else {
            TransactionManager transactionManager = getTransactionManager();
            if (transactionManager == null) {
                return false;
            } else {
                return transactionManager.isTransactional();
            }
        }
    }

    @SneakyThrows
    private TransactionManager getTransactionManager() {
        if (txManagerField == null) return null;
        TransactionManager transactionManager = (TransactionManager) txManagerField.get(producer);
        return transactionManager;
    }

    @SneakyThrows
    protected boolean isTransactionCompleting() {
        if (producer instanceof MockProducer) return false;
        return (boolean) txManagerMethodIsCompleting.invoke(getTransactionManager());
    }

    @SneakyThrows
    protected boolean isTransactionReady() {
        if (producer instanceof MockProducer) return true;
        return (boolean) txManagerMethodIsReady.invoke(getTransactionManager());
    }

    @Override
    public void initTransactions() {
        log.debug("Init transaction...");
        producer.initTransactions();
        this.producerState = INIT;
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        log.debug("Beginning transaction...");
        producer.beginTransaction();
        this.producerState = BEGIN;
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        log.debug("Committing transaction...");
        producer.commitTransaction();
        this.producerState = COMMIT;
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        log.debug("Aborting transaction...");
        producer.abortTransaction();
        this.producerState = ABORT;
    }

    @Override
    public void close() {
        producer.close();
        this.producerState = CLOSE;
    }

    @Override
    public void close(final Duration timeout) {
        producer.close(timeout);
        this.producerState = CLOSE;
    }

    /**
     * According to our state tracking, does the Producer have an open transaction
     *
     * @return true if there's an open transaction
     */
    public boolean isTransactionOpen() {
        return this.producerState.equals(BEGIN);
    }
}

package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;
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
import java.util.Map;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@RequiredArgsConstructor
public class ProducerWrap<K, V> implements Producer<K, V> {

    private final ParallelConsumerOptions<K, V> options;

    /**
     * todo docs
     */
//    @Getter
    private final boolean producerIsConfiguredForTransactions;

    // nasty reflection
    private Field txManagerField;
    private Method txManagerMethodIsCompleting;
    private Method txManagerMethodIsReady;

    @Delegate(excludes = Excludes.class)
    private final Producer producer;

    public ProducerWrap(ParallelConsumerOptions options) {
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

    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }


    /**
     * @return boolean which shows if we are set up for transactions or not
     */
    // todo rename
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
     * Nasty reflection but better than relying on user supplying their config
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

    /**
     * TODO talk about alternatives to this brute force approach for retrying committing transactions
     */
    @SneakyThrows
    protected boolean isTransactionCompleting() {
        if (producer instanceof MockProducer) return false;
        return (boolean) txManagerMethodIsCompleting.invoke(getTransactionManager());
    }

    /**
     * TODO talk about alternatives to this brute force approach for retrying committing transactions
     */
    @SneakyThrows
    protected boolean isTransactionReady() {
        if (producer instanceof MockProducer) return true;
        return (boolean) txManagerMethodIsReady.invoke(getTransactionManager());
    }


}
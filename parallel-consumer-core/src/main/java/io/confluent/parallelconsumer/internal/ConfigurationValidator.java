package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;
import java.util.Set;

import static java.lang.Boolean.TRUE;

/**
 * @author Antony Stubbs
 */
public class ConfigurationValidator {

    private void validateConfiguration() {
        options.validate();

        validateConsumer();
    }

    private void validateConsumer() {
        checkGroupIdConfigured(consumer);
        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);
    }

    private void checkGroupIdConfigured(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            consumer.groupMetadata();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                    "configured GroupId on your Consumer?", e);
        }
    }

    private void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumerToCheck) {
        if (consumerToCheck instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumerToCheck.subscription();
        Set<TopicPartition> assignment = consumerToCheck.assignment();
        if (!subscription.isEmpty() || !assignment.isEmpty()) {
            throw new IllegalStateException("Consumer subscription must be managed by the Parallel Consumer. Use " + this.getClass().getName() + "#subcribe methods instead.");
        }
    }

    /**
     * Nasty reflection to check if auto commit is disabled.
     * <p>
     * Other way would be to politely request the user also include their consumer properties when construction, but
     * this is more reliable in a correctness sense, but brittle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    private void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            if (consumer instanceof KafkaConsumer) {
                // Could use Commons Lang FieldUtils#readField - but, avoid needing commons lang
                Field coordinatorField = KafkaConsumer.class.getDeclaredField("coordinator");
                coordinatorField.setAccessible(true);
                ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

                if (coordinator == null)
                    throw new IllegalStateException("Coordinator for Consumer is null - missing GroupId? Reflection broken?");

                Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
                autoCommitEnabledField.setAccessible(true);
                Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

                if (TRUE.equals(isAutoCommitEnabled))
                    throw new ParallelConsumerException("Consumer auto commit must be disabled, as commits are handled by the library.");
            } else if (consumer instanceof MockConsumer) {
                log.debug("Detected MockConsumer class which doesn't do auto commits");
            } else {
                // Probably Mockito
                log.error("Consumer is neither a KafkaConsumer nor a MockConsumer - cannot check auto commit is disabled for consumer type: " + consumer.getClass().getName());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Cannot check auto commit is disabled for consumer type: " + consumer.getClass().getName(), e);
        }
    }
}

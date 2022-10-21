package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * An implementation of the Kafka Consumer API, with unsupported functions which throw exceptions. Useful for when you
 * want to let something else use PC, where it's unaware of it's capaiblities, yet you know it won't call unsupported
 * methods.
 *
 * @author Antony Stubbs
 */
public interface PartialConsumerApi extends Consumer {
}

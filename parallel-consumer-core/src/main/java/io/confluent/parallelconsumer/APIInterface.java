package io.confluent.parallelconsumer;

/**
 * Controls for the engine
 *
 * @author Antony Stubbs
 */
public interface APIInterface {

    /**
     * Access and control the org.apache.kafka.clients.consumer.Consumer fix
     */
    ConsumerApi consumer();

    /**
     * Pause the engine
     */
    void pause();

    /**
     * Resume the engine
     */
    void resume();
}

package io.confluent.parallelconsumer;

/**
 * @author Antony Stubbs
 */
public interface APIInterface {
    ConsumerApi consumer();

    void pause();

    void resume();
}

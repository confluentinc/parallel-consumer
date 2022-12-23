package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.Consumer;

/**
 * An implementation of all of the full {@link Consumer} interface, where methods that aren't supported either throw an
 * exception or swallows the call with a log.
 * <p>
 * Useful for when you want to let something else use or manipulate PC or it's wrapped {@link Consumer}, but where it's
 * unaware of it's capabilities, AND you know it won't call unsupported methods - or if it does not, you just set it to
 * no-op.
 * <p>
 * Generally, you can only... And you can't...
 * <p>
 * todo list cans and cants generally
 *
 * @param <K>
 * @param <V>
 * @author Antony Stubbs
 * @see Consumer
 * @see ConsumerFacadeForPC
 * @see PCConsumerAPIStrict
 */
public interface FullConsumerFacade<K, V> extends Consumer<K, V> {

    /**
     * Set the reaction for unsupported methods to either throw an exception or swallow the call with a log.
     */
    void setReactionMode(FullConsumerFacadeImpl.UnsupportedReaction unsupportedReactionMode);

}

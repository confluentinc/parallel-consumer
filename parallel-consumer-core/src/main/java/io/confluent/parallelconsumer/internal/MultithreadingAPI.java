package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * Convention only indicator that something is ThreadSafe.
 * <p>
 * All these methods should go through the {@link io.confluent.csid.actors.Actor} system.
 *
 * @author Antony Stubbs
 */
public interface MultithreadingAPI {

}

package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;

/**
 * Internal thread safe API for the Controller
 *
 * @author Antony Stubbs
 */
//todo merge with ControllerPackageAPI?
//todo extract the other interfaces
public interface ControllerInternalAPI<K, V> extends ThreadSafeAPI {

    void sendWorkResultAsync(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc);

}

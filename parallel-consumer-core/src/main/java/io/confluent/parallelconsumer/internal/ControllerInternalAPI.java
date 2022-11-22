package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;

/**
 * Internal thread safe API for the Controller
 *
 * @author Antony Stubbs
 */
//todo extract the other interfaces
public interface ControllerInternalAPI<K, V> extends ThreadSafeAPI {

    void sendWorkResultAsync(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc);

}

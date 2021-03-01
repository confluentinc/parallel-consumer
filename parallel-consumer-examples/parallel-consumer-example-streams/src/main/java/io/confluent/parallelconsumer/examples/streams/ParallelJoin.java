package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */


import io.confluent.parallelconsumer.ParallelConsumer;
import org.apache.kafka.streams.state.KeyValueStore;

public class ParallelJoin {

    /**
     * Needs KeyValueStore injected.
     */
    ParallelJoin(KeyValueStore<UserId, UserProfile> store, ParallelConsumer<UserId, UserEvent> pc) {
        pc.poll(record -> {
            UserId userId = record.key();
            UserEvent userEvent = record.value();

            UserProfile userDeviceTokenRegistry = store.get(userId);
            if (userDeviceTokenRegistry != null) {
                // join hit
                // create payload with even details and call third party system, or produce a result message
                userEvent.getEventPayload();
                //....
            } else {
                // join miss
                // drop - not registered devices for that user
            }
        });
    }

}

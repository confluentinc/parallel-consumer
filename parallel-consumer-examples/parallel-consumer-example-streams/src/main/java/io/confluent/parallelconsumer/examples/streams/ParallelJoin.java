package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */


import io.confluent.parallelconsumer.ParallelConsumer;
import org.apache.kafka.streams.state.KeyValueStore;

public class ParallelJoin {

    // tag::example[]
    /**
     * Needs KeyValueStore injected.
     */
    ParallelJoin(KeyValueStore<UserId, UserProfile> store, ParallelConsumer<UserId, UserEvent> pc) {
        pc.poll(record -> {
            UserId userId = record.key();
            UserEvent userEvent = record.value();

            UserProfile userProfile = store.get(userId);
            if (userProfile != null) { // <1>
                // join hit
                // create payload with even details and call third party system, or produce a result message
                userEvent.getEventPayload();
                //....
            } else { // <2>
                // join miss
                // drop - not registered devices for that user
            }
        });
    }
    // end::example[]

}

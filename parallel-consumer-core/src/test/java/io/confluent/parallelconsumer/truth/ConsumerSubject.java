package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedSubject;
import one.util.streamex.StreamEx;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerParentSubject;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Generated;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static io.confluent.parallelconsumer.truth.CommitHistorySubject.commitHistories;

/**
 * Optionally move this class into source control, and add your custom assertions here.
 * <p>
 * If the system detects this class already exists, it won't attempt to generate a new one. Note that if the base
 * skeleton of this class ever changes, you won't automatically get it updated.
 *
 * @author Antony Stubbs
 * @see Consumer
 * @see ConsumerParentSubject
 */
@UserManagedSubject(Consumer.class)
@Generated(value = "io.stubbs.truth.generator.internal.TruthGenerator", date = "2022-05-17T12:20:38.207945Z")
public class ConsumerSubject extends ConsumerParentSubject {

    protected ConsumerSubject(FailureMetadata failureMetadata, org.apache.kafka.clients.consumer.Consumer actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link Consumer} class.
     */
    @SubjectFactoryMethod
    public static Factory<ConsumerSubject, Consumer> consumers() {
        return ConsumerSubject::new;
    }

    private final Duration timeout = Duration.ofSeconds(10);

    public CommitHistorySubject hasCommittedToPartition(TopicPartition topicPartitions) {
        Map<TopicPartition, CommitHistorySubject> map = hasCommittedToPartition(UniSets.of(topicPartitions));
        return map.values().stream()
                .findFirst()
                .orElse(
                        check("getCommitHistory(%s)", topicPartitions.topic())
                                .about(commitHistories())
                                .that(new CommitHistory(UniLists.of())));
    }

    public Map<TopicPartition, CommitHistorySubject> hasCommittedToPartition(Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> committed = actual.committed(partitions, timeout);
        return StreamEx.of(committed.entrySet())
                .filter(entry -> entry.getValue() != null)
                .toMap(entry -> entry.getKey(), entry
                        -> check("getCommitHistory(%s)", entry.getKey().topic() + ":" + entry.getKey().partition())
                        .about(commitHistories())
                        .that(new CommitHistory(UniLists.of(entry.getValue()))));
    }

}

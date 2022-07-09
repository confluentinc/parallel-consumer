package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedTruth;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerParentSubject;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import javax.annotation.Generated;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static io.confluent.parallelconsumer.truth.CommitHistorySubject.commitHistories;

/**
 * Optionally move this class into source control, and add your custom assertions here.
 *
 * <p>
 * If the system detects this class already exists, it won't attempt to generate a new one. Note that if the base
 * skeleton of this class ever changes, you won't automatically get it updated.
 *
 * @see Consumer
 * @see ConsumerParentSubject
 */
@UserManagedTruth(Consumer.class)
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

    public CommitHistorySubject hasCommittedToPartition(NewTopic topic) {
        int defaultPartition = 0;
        TopicPartition tp = new TopicPartition(topic.name(), defaultPartition);
        var committed = (Map<TopicPartition, OffsetAndMetadata>) actual.committed(UniSets.of(tp), timeout);
        var offsets = Optional.ofNullable(committed.get(tp));

        var history = offsets.isPresent()
                ? UniLists.of(offsets.get())
                : UniLists.<OffsetAndMetadata>of();

        return check("getCommitHistory(%s)", topic.name() + ":" + defaultPartition).about(commitHistories()).that(new CommitHistory(history));
    }

//    @Override
//    protected String actualCustomStringRepresentation() {
//        String assignors = ReflectionToStringBuilder.toStringExclude(actual,
//                "assignors", "client", "time", "kafkaConsumerMetrics", "coordinator", "log", "metrics", "fetcher",
//                "keyDeserializer", "valueDeserializer", "interceptors", "cachedSubscriptionHashAllFetchPositions", "metadata"
//        );
//        return assignors;
//    }

}

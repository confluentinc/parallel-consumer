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
import pl.tlinkowski.unij.api.UniSets;

import javax.annotation.Generated;
import java.time.Duration;
import java.util.List;

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

    private final Duration timeout = Duration.ofSeconds(10);

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

    public CommitHistorySubject hasCommittedToPartition(NewTopic topic) {
        var committed = actual.committed(UniSets.of(topic), timeout);
        List<OffsetAndMetadata> offsets = (List<OffsetAndMetadata>) committed.get(topic);
        CommitHistory commitHistory = new CommitHistory(offsets);
        return check("getCommitHistory(%s)", topic).about(commitHistories()).that(commitHistory);
    }
}

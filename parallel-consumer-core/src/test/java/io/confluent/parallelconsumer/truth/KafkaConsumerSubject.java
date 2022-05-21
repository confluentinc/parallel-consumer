package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedTruth;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumerParentSubject;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.List;

import static io.confluent.parallelconsumer.truth.CommitHistorySubject.commitHistories;

/**
 * @see KafkaConsumer
 * @see KafkaConsumerParentSubject
 */
@UserManagedTruth(KafkaConsumer.class)
public class KafkaConsumerSubject extends KafkaConsumerParentSubject {

    private final Duration timeout = Duration.ofSeconds(10);

    protected KafkaConsumerSubject(FailureMetadata failureMetadata,
                                   org.apache.kafka.clients.consumer.KafkaConsumer actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link KafkaConsumer} class.
     */
    @SubjectFactoryMethod
    public static Factory<KafkaConsumerSubject, KafkaConsumer> kafkaConsumers() {
        return KafkaConsumerSubject::new;
    }

    public CommitHistorySubject hasCommittedToPartition(NewTopic topic) {
        var committed = actual.committed(UniSets.of(topic), timeout);
        List<OffsetAndMetadata> offsets = committed.get(topic);
        CommitHistory commitHistory = new CommitHistory(offsets);
        return check("getCommitHistory(%s)", topic).about(commitHistories()).that(commitHistory);
    }
}

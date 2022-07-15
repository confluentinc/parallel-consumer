package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorChildSubject;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorParentSubject;
import io.confluent.parallelconsumer.internal.ConsumerFacade;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Main Subject for the class under test.
 *
 * @see ParallelEoSStreamProcessor
 * @see ParallelEoSStreamProcessorParentSubject
 * @see ParallelEoSStreamProcessorChildSubject
 */
@UserManagedSubject(ParallelEoSStreamProcessor.class)
public class ParallelEoSStreamProcessorSubject extends ParallelEoSStreamProcessorParentSubject
        implements UserManagedMiddleSubject {

    private final Duration timeout = Duration.ofSeconds(10);

    protected ParallelEoSStreamProcessorSubject(FailureMetadata failureMetadata,
                                                ParallelEoSStreamProcessor actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link ParallelEoSStreamProcessor} class.
     */
    @SubjectFactoryMethod
    public static Factory<ParallelEoSStreamProcessorSubject, ParallelEoSStreamProcessor> parallelEoSStreamProcessors() {
        return ParallelEoSStreamProcessorSubject::new;
    }


    public CommitHistorySubject hasCommittedToPartition(NewTopic topic) {
        return getConsumer().hasCommittedToPartition(topic);
    }

    public CommitHistorySubject hasCommittedToAnyAssignedPartitionOf(NewTopic newTopic) {
        isNotNull();
        ConsumerFacade consumer = actual.getConsumerFacade();
        return check("getConsumerFacade()")
                .about(ConsumerSubject.consumers())
                .that(consumer)
                .hasCommittedToPartition(newTopic);
    }

    public Map<TopicPartition, CommitHistorySubject> hasCommittedToAnyAssignedPartitionOf(Set<NewTopic> newTopic) {
        isNotNull();
        ConsumerFacade consumer = actual.getConsumerFacade();
        return check("getConsumerFacade()")
                .about(ConsumerSubject.consumers())
                .that(consumer)
                .hasCommittedToPartition(newTopic);
    }

    public CommitHistorySubject hasCommittedToAnyAssignedPartitionOf(String topicName) {
    }

    public void hasCommittedToAnything(int offset) {

    }
}

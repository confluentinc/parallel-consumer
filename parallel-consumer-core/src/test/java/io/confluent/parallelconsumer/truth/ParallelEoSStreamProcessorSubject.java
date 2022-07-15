package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorChildSubject;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorParentSubject;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;

import java.time.Duration;

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

    public CommitHistorySubject hasCommittedToAnyAssignedPartitionOf(String topicName) {
        return null;
    }

    public void hasCommittedToAnything(int offset) {

    }
}

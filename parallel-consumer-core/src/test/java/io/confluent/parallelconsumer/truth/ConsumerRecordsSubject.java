package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecordsChildSubject;
import org.apache.kafka.clients.consumer.ConsumerRecordsParentSubject;

/**
 * @author Antony Stubbs
 * @see ConsumerRecords
 * @see ConsumerRecordsParentSubject
 * @see ConsumerRecordsChildSubject
 */
@UserManagedSubject(ConsumerRecords.class)
public class ConsumerRecordsSubject extends ConsumerRecordsParentSubject implements UserManagedMiddleSubject {

    protected ConsumerRecordsSubject(FailureMetadata failureMetadata,
                                     org.apache.kafka.clients.consumer.ConsumerRecords actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link ConsumerRecords} class.
     */
    @SubjectFactoryMethod
    public static Factory<ConsumerRecordsSubject, ConsumerRecords> consumerRecordses() {
        return ConsumerRecordsSubject::new;
    }

    public void containsOffset(int blockFreeRecords) {

    }

    public void doesntContainOffset(int blockedOffset) {

    }
}

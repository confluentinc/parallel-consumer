package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.internal.ProducerManagerChildSubject;
import io.confluent.parallelconsumer.internal.ProducerManagerParentSubject;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;

/**
 * Main Subject for the class under test.
 *
 * @see ProducerManager
 * @see ProducerManagerParentSubject
 * @see ProducerManagerChildSubject
 */
@UserManagedSubject(ProducerManager.class)
public class ProducerManagerSubject extends ProducerManagerParentSubject implements UserManagedMiddleSubject {

    protected ProducerManagerSubject(FailureMetadata failureMetadata, ProducerManager actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link ProducerManager} class.
     */
    @SubjectFactoryMethod
    public static Factory<ProducerManagerSubject, ProducerManager> producerManagers() {
        return ProducerManagerSubject::new;
    }

    public void transactionNotOpen() {
        throw new InternalRuntimeError("");
    }

    public void transactionOpen() {
        throw new InternalRuntimeError("");
    }
}

package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.internal.ProducerManagerChildSubject;
import io.confluent.parallelconsumer.internal.ProducerManagerParentSubject;
import io.confluent.parallelconsumer.internal.ProducerWrapper;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;

/**
 * Main Subject for the class under test.
 *
 * @author Antony Stubbs
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
        check("isTransactionOpen()").that(actual.getProducerWrapper().isTransactionOpen()).isFalse();
    }

    public void transactionOpen() {
        check("isTransactionOpen()").that(actual.getProducerWrapper().isTransactionOpen()).isTrue();
    }

    public void stateIs(ProducerWrapper.ProducerState targetState) {
        var producerWrap = actual.getProducerWrapper();
        var producerState = producerWrap.getProducerState();
        check("getProducerState()").that(producerState).isEqualTo(targetState);
    }

}

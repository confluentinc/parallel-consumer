package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorChildSubject;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorParentSubject;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;

/**
 * Main Subject for the class under test.
 *
 * @author Antony Stubbs
 * @see ParallelEoSStreamProcessor
 * @see ParallelEoSStreamProcessorParentSubject
 * @see ParallelEoSStreamProcessorChildSubject
 */
@UserManagedSubject(ParallelEoSStreamProcessor.class)
public class ParallelEoSStreamProcessorSubject extends ParallelEoSStreamProcessorParentSubject
        implements UserManagedMiddleSubject {

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


    public void hasCommittedToAnything(int offset) {
        throw new InternalRuntimeError("");
    }

}

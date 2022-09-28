package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import io.confluent.parallelconsumer.state.PartitionState;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecordsChildSubject;
import org.apache.kafka.clients.consumer.ConsumerRecordsParentSubject;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
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

    public void hasHeadOffsetAnyTopicPartition(int target) {
        long highestOffset = findHighestOffsetAnyPartition(target);
        check("headOffset").that(highestOffset).isAtLeast(target);
    }

    private long findHighestOffsetAnyPartition(int target) {
        var iterator = getConsumerRecordIterator();
        long highestOffset = PartitionState.KAFKA_OFFSET_ABSENCE;
        while (iterator.hasNext()) {
            ConsumerRecord<?, ?> next = iterator.next();
            long offset = next.offset();
            highestOffset = offset;
            if (offset >= target) {
                break;
            }
        }
        return highestOffset;
    }

    @NotNull
    private Iterator<ConsumerRecord<?, ?>> getConsumerRecordIterator() {
        return (Iterator<ConsumerRecord<?, ?>>) actual.iterator();
    }
}

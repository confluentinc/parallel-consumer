package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.OptionalSubject;
import com.google.common.truth.Subject;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedSubject;
import lombok.ToString;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertAbout;

/**
 * @author Antony Stubbs
 * @see CommitHistory
 */
@ToString
@UserManagedSubject(CommitHistory.class)
public class CommitHistorySubject extends Subject {
    private final CommitHistory actual;

    protected CommitHistorySubject(FailureMetadata metadata, CommitHistory actual) {
        super(metadata, actual);
        this.actual = actual;
    }

    @SubjectFactoryMethod
    public static Factory<CommitHistorySubject, CommitHistory> commitHistories() {
        return CommitHistorySubject::new;
    }

    public static CommitHistorySubject assertTruth(final CommitHistory actual) {
        return assertThat(actual);
    }

    public static CommitHistorySubject assertThat(final CommitHistory actual) {
        return assertAbout(commitHistories()).that(actual);
    }

    public void atLeastOffset(long needleCommit) {
        Optional<Long> highestCommitOpt = this.actual.highestCommit();
        check("highestCommit()").about(OptionalSubject.optionals())
                .that(highestCommitOpt)
                .isPresent();
        check("highestCommit().atLeastOffset()")
                .that(highestCommitOpt.get())
                .isAtLeast(needleCommit);
    }

    public void offset(long quantity) {
        check("getOffsetHistory()").that(actual.getOffsetHistory()).contains(quantity);
    }

    public void anything() {
        check("commits()").that(actual.getOffsetHistory()).isNotEmpty();
    }

    public void nothing() {
        check("commits()").that(actual.getOffsetHistory()).isEmpty();
    }

    public void isEmpty() {
        nothing();
    }

    /**
     * Asserts that the given offsets are in the offset metadata as incomplete.
     */
    public void encodedIncomplete(int... expectedEncodedOffsetsArray) {
        Set<Long> incompleteOffsets = actual.getEncodedSucceeded().getIncompleteOffsets();
        check("encodedSucceeded()")
                .that(incompleteOffsets)
                .containsExactlyElementsIn(Arrays.stream(expectedEncodedOffsetsArray)
                        .boxed()
                        .map(Long::valueOf)
                        .collect(Collectors.toList()));
    }

    public void encodingEmpty() {
        check("encodedMetadata()").that(actual.getEncoding()).isEmpty();
    }
}

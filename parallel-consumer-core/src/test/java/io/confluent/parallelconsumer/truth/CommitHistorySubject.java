package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.OptionalSubject;
import com.google.common.truth.Subject;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedTruth;

import java.util.Optional;

import static com.google.common.truth.Truth.assertAbout;

/**
 * @see CommitHistory
 */
@UserManagedTruth(CommitHistory.class)
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

    public void atLeastOffset(int needleCommit) {
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
//
//    @Override
//    protected String actualCustomStringRepresentation() {
////        String s = actual.;
//        return super.actualCustomStringRepresentation();
//    }
}

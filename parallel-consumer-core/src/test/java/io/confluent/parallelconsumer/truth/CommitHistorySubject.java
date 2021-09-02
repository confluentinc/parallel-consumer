package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.OptionalSubject;
import com.google.common.truth.Subject;
import io.confluent.parallelconsumer.model.CommitHistory;

import static com.google.common.truth.Truth.assertAbout;

public class CommitHistorySubject extends Subject {
    private final CommitHistory actual;

    protected CommitHistorySubject(FailureMetadata metadata, CommitHistory actual) {
        super(metadata, actual);
        this.actual = actual;
    }

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
        check("atLeastOffset()").about(OptionalSubject.optionals()).that(actual.highestCommit()).isPresent();
        check("atLeastOffset()").that(actual.highestCommit().get()).isAtLeast(needleCommit);
    }

    public void offset(long quantity) {
        check("atLeastOffset()").that(actual.getOffsetHistory()).contains(quantity);
    }
}

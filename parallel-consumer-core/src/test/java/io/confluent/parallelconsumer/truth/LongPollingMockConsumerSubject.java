package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.model.CommitHistory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertAbout;
import static io.confluent.parallelconsumer.truth.CommitHistorySubject.commitHistories;

public class LongPollingMockConsumerSubject<K, V> extends Subject {
    private final LongPollingMockConsumer<K, V> actual;

    protected LongPollingMockConsumerSubject(FailureMetadata metadata, LongPollingMockConsumer actual) {
        super(metadata, actual);
        this.actual = actual;
    }

    public static Factory<LongPollingMockConsumerSubject, LongPollingMockConsumer> mockConsumers() {
        return LongPollingMockConsumerSubject::new;
    }

    public static LongPollingMockConsumerSubject assertTruth(final LongPollingMockConsumer actual) {
        return assertThat(actual);
    }

    public static LongPollingMockConsumerSubject assertThat(final LongPollingMockConsumer actual) {
        return assertAbout(mockConsumers()).that(actual);
    }

    public CommitHistorySubject hasCommittedToPartition(TopicPartition tp) {
        isNotNull();
        CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>> allCommits = actual.getCommitHistoryInt();
        List<OffsetAndMetadata> historyForCommitsToPartition = allCommits.stream()
                .filter(aCommitInstance -> aCommitInstance.containsKey(tp))
                .map(aCommitInstance -> aCommitInstance.get(tp))
                .collect(Collectors.toList());
        CommitHistory commitHistory = new CommitHistory(historyForCommitsToPartition);
        return check("getCommitHistory(%s)", tp).about(commitHistories()).that(commitHistory);
    }


}

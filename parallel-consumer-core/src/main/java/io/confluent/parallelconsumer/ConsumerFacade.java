package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * todo docs
 */
public class ConsumerFacade implements Consumer {
    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public Set<String> subscription() {
        return null;
    }

    @Override
    public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(final Pattern pattern) {

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {

    }

    @Override
    public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public long position(final TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return 0;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public OptionalLong currentLag(final TopicPartition topicPartition) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return null;
    }

    @Override
    public void enforceRebalance() {

    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection collection, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection collection) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection collection, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection collection) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map timestampsToSearch, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set set, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set set) {
        return null;
    }

    @Override
    public void seekToEnd(final Collection collection) {

    }

    @Override
    public void seekToBeginning(final Collection collection) {

    }

    @Override
    public void assign(final Collection collection) {

    }

    @Override
    public void subscribe(final Collection topics, final ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(final Collection topics) {

    }

    // no-ops

}

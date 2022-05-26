package io.confluent.parallelconsumer;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class ConsumerFacade implements Consumer {

    /// above
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
    public OffsetAndMetadata committed(final TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
        return null;
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
    public void close() {

    }

    @Override
    public void close(final Duration timeout) {

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


    @Override
    public ConsumerRecords poll(final long timeout) {
        return null;
    }

    @Override
    public ConsumerRecords poll(final Duration timeout) {
        return null;
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(final Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(final Map offsets, final OffsetCommitCallback callback) {

    }

    @Override
    public void commitSync(final Map offsets, final Duration timeout) {

    }

    @Override
    public void commitSync(final Map offsets) {

    }

    @Override
    public void resume(final Collection collection) {

    }

    @Override
    public void pause(final Collection collection) {

    }

    @Override
    public void wakeup() {

    }
}

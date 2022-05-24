package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static lombok.AccessLevel.PROTECTED;

@Slf4j
@RequiredArgsConstructor
public class ShardCollection<K, V> {

    private final ParallelConsumerOptions options;

    private final WorkManager<K, V> wm;

    /**
     * Map of keys to TP to shard
     * <p>
     * Object Type is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see ProcessingShard
     * @see WorkManager#getWorkIfAvailable()
     */
    // performance: could disable/remove if using partition order - but probably not worth the added complexity in the code to handle an extra special case
    //        todo need to use TP actually, so that IF KEY mode, and same keys exists on multiple partitions, they don't overwrite each other, and progress is still made
//        - need to also add indirection to SM to map my TP
//    private final Map<ShardKey, Map<TopicPartition, ProcessingShard<K, V>>> processingShards = new ConcurrentHashMap<>();
    // todo blocked by PR#270 merge
    @Getter(PROTECTED)
    private final Map<ShardKey, ProcessingShard<K, V>> processingShards = new HashMap<>();

    ShardKey computeShardKey(WorkContainer<?, ?> wc) {
        return ShardKey.of(wc, options);
    }

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    public Optional<ProcessingShard<K, V>> getShard(ShardKey key) {
        var shard = processingShards.get(key);
//        return Optional.ofNullable(topicPartitionProcessingShardMap.get(topicPartition));
        return Optional.ofNullable(shard);
    }

    public Optional<ProcessingShard<K, V>> getShard(WorkContainer<?, ?> workContainer) {
        ShardKey key = computeShardKey(workContainer);
        return getShard(key);
    }

    public Collection<ProcessingShard<K, V>> values() {
        return null;
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        ShardKey shardKey = computeShardKey(wc);
        var shard = processingShards.computeIfAbsent(shardKey,
                ignore -> new ProcessingShard<>(shardKey, options, wm.getPm()));
        shard.addWorkContainer(wc);
    }


    public void removeShardIfEmpty(WorkContainer<?, ?> workContainer) {
        ShardKey key = computeShardKey(workContainer);
        Optional<ProcessingShard<K, V>> shardOpt = getShard(key);

        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shardOpt.isPresent() && shardOpt.get().isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            this.processingShards.remove(key);
        }
    }

    void removeShardFor(final WorkContainer<K, V> work) {
        ShardKey shardKey = computeShardKey(work);

        if (processingShards.containsKey(shardKey)) {
            ProcessingShard<K, V> shard = processingShards.get(shardKey);
            shard.remove(work);
            removeShardIfEmpty(work);
        } else {
            log.trace("Shard referenced by WC: {} with shard key: {} already removed", work, shardKey);
        }
    }
}

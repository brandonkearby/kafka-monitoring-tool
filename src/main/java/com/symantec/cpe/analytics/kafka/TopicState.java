package com.symantec.cpe.analytics.kafka;

import java.util.*;

/**
 * @author Brandon Kearby
 *         February 17 2017.
 */
public class TopicState {

    private Topic topic;
    private Map<Partition, TopicPartitionState> partitionTopicStateMap;


    private static class TopicPartitionState {
        Long firstOffset;
        Long lastOffset;
        Long firstOffsetTime;
        Long lastOffsetTime;
    }

    public TopicState(Topic topic) {
        Objects.requireNonNull(topic, "Topic can't be null");
        this.topic = topic;
        partitionTopicStateMap = new TreeMap<>();
    }

    public Topic getTopic() {
        return topic;
    }

    public Set<Partition> getPartitions() {
        return new HashSet<>(partitionTopicStateMap.keySet());
    }

    public Long getFirstOffset(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return getOrCreateTopicPartitionState(partition).firstOffset;
    }

    public Long getFirstOffsetTime(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return getOrCreateTopicPartitionState(partition).firstOffsetTime;
    }

    private TopicPartitionState getOrCreateTopicPartitionState(Partition partition) {
        TopicPartitionState topicPartitionState = partitionTopicStateMap.get(partition);
        if (topicPartitionState == null) {
            topicPartitionState = new TopicPartitionState();
            partitionTopicStateMap.put(partition, topicPartitionState);
        }
        return topicPartitionState;
    }

    public void setFirstOffset(Partition partition, Long offset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(offset, "Offset can't be null");
        getOrCreateTopicPartitionState(partition).firstOffset = offset;
    }

    public void setLastOffset(Partition partition, Long lastOffset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(lastOffset, "Offset can't be null");
        getOrCreateTopicPartitionState(partition).lastOffset = lastOffset;
    }
    public void setFirstOffsetTime(Partition partition, Long offset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(offset, "Offset can't be null");
        getOrCreateTopicPartitionState(partition).firstOffsetTime = offset;
    }

    public void setLastOffsetTime(Partition partition, Long lastOffset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(lastOffset, "Offset can't be null");
        getOrCreateTopicPartitionState(partition).lastOffsetTime = lastOffset;
    }

    public Long getLastOffset(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return getOrCreateTopicPartitionState(partition).lastOffset;
    }

    public Long getLastOffsetTime(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return getOrCreateTopicPartitionState(partition).lastOffsetTime;
    }
}

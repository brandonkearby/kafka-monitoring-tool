package com.symantec.cpe.analytics.kafka;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Brandon Kearby
 *         February 17 2017.
 */
public class TopicState {

    private Topic topic;
    private Map<Partition, Long> firstOffsets;
    private Map<Partition, Long> lastOffsets;

    public TopicState(Topic topic) {
        Objects.requireNonNull(topic, "Topic can't be null");
        this.topic = topic;
        firstOffsets = new TreeMap<>();
        lastOffsets = new TreeMap<>();
    }

    public Topic getTopic() {
        return topic;
    }

    public Set<Partition> getPartitions() {
        return firstOffsets.keySet();
    }

    public Long getFirstOffset(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return firstOffsets.get(partition);
    }

    public void setFirstOffset(Partition partition, Long offset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(offset, "Offset can't be null");
        firstOffsets.put(partition, offset);
    }

    public void setLastOffset(Partition partition, Long lastOffset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(lastOffset, "Offset can't be null");
        lastOffsets.put(partition, lastOffset);
    }

    public Long getLastOffset(Partition partition) {
        Objects.requireNonNull(partition, "Partition can't be null");
        return lastOffsets.get(partition);
    }
}

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
    private Map<Partition, Long> offsets;

    public TopicState(Topic topic) {
        Objects.requireNonNull(topic, "Topic can't be null");
        this.topic = topic;
        offsets = new TreeMap<>();
    }

    public Topic getTopic() {
        return topic;
    }

    public Set<Partition> getPartitions() {
        return offsets.keySet();
    }

    public Long getOffset(Partition partition) {
        return offsets.get(partition);
    }

    public void setOffset(Partition partition, Long offset) {
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(offset, "Offset can't be null");
        offsets.put(partition, offset);
    }
}

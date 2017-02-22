package com.symantec.cpe.analytics.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class ClusterState {
    private Map<ConsumerGroup, ConsumerGroupState> groupState = new HashMap<>();
    private Map<Topic, TopicState> topicState = new HashMap<>();

    public Set<Topic> getTopics() {
        return topicState.keySet();
    }

    public Set<ConsumerGroup> getConsumerGroups() {
        return groupState.keySet();
    }

    public ConsumerGroupState get(ConsumerGroup consumerGroup) {
        Objects.requireNonNull(consumerGroup, "ConsumerGroup can't be null");
        return groupState.get(consumerGroup);
    }

    public void setConsumerGroupState(ConsumerGroup consumerGroup, ConsumerGroupState consumerGroupState) {
        groupState.put(consumerGroup, consumerGroupState);
    }

    public void setTopicState(Topic topic, Partition partition, Long firstOffset, Long lastOffset) {
        Objects.requireNonNull(topic, "Topic can't be null");
        Objects.requireNonNull(partition, "Partition can't be null");
        Objects.requireNonNull(firstOffset, "Offset can't be null");

        TopicState ts = this.topicState.get(topic);
        if (ts == null) {
            ts = new TopicState(topic);
            this.topicState.put(topic, ts);
        }
        ts.setFirstOffset(partition, firstOffset);
        ts.setLastOffset(partition, lastOffset);
    }

    public TopicState getTopicState(Topic topic) {
        Objects.requireNonNull(topic, "Topic can't be null");
        return topicState.get(topic);
    }
}

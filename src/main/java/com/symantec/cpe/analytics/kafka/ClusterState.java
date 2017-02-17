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

    public Set<ConsumerGroup> getConsumerGroups() {
        return groupState.keySet();
    }

    public ConsumerGroupState get(ConsumerGroup consumerGroup) {
        Objects.requireNonNull(consumerGroup, "ConsumerGroup can't be null");
        return groupState.get(consumerGroup);
    }

    public void put(ConsumerGroup consumerGroup, ConsumerGroupState consumerGroupState) {
        groupState.put(consumerGroup, consumerGroupState);
    }
}

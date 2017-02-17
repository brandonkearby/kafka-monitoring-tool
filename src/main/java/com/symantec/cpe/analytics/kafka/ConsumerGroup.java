package com.symantec.cpe.analytics.kafka;

import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class ConsumerGroup {
    String groupId;

    public ConsumerGroup(String groupId) {
        Objects.requireNonNull(groupId, "GroupId can't be null");
        this.groupId = groupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroup consumerGroup = (ConsumerGroup) o;

        return groupId.equals(consumerGroup.groupId);
    }

    @Override
    public int hashCode() {
        return groupId.hashCode();
    }

    @Override
    public String toString() {
        return groupId;
    }
}

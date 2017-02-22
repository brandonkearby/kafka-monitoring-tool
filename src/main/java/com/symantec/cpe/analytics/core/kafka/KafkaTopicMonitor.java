package com.symantec.cpe.analytics.core.kafka;

/**
 * @author Brandon Kearby
 *         February 21 2017.
 */
public class KafkaTopicMonitor {
    private String topic;
    private Integer partition;
    private long firstOffset;
    private long lastOffset;
    private long logSize;

    public KafkaTopicMonitor(String topic, Integer partition, long firstOffset, long lastOffset, long logSize) {
        this.topic = topic;
        this.partition = partition;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.logSize = logSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public long getLogSize() {
        return logSize;
    }

    public void setLogSize(long logSize) {
        this.logSize = logSize;
    }

    @Override
    public String toString() {
        return "KafkaTopicMonitor{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", logSize=" + logSize +
                '}';
    }
}

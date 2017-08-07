package com.symantec.cpe.analytics.core.kafka;

import org.joda.time.DateTime;

/**
 * @author Brandon Kearby
 *         February 21 2017.
 */
public class KafkaTopicMonitor {
    private String topic;
    private Integer partition;
    private long firstOffset;
    private long lastOffset;
    private long firstOffsetTime;
    private String firstOffsetTimePretty;
    private long lastOffsetTime;
    private String lastOffsetTimePretty;
    private long logSize;

    public KafkaTopicMonitor(String topic, Integer partition, long firstOffset, long firstOffsetTime, long lastOffset, long lastOffsetTime, long logSize) {
        this.topic = topic;
        this.partition = partition;
        this.firstOffset = firstOffset;
        this.firstOffsetTime = firstOffsetTime;
        this.lastOffset = lastOffset;
        this.lastOffsetTime = lastOffsetTime;
        this.logSize = logSize;
        this.firstOffsetTimePretty = new DateTime(this.firstOffsetTime).toString();
        this.lastOffsetTimePretty = new DateTime(this.lastOffsetTime).toString();
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

    public long getFirstOffsetTime() {
        return firstOffsetTime;
    }

    public void setFirstOffsetTime(long firstOffsetTime) {
        this.firstOffsetTime = firstOffsetTime;
    }

    public String getFirstOffsetTimePretty() {
        return firstOffsetTimePretty;
    }

    public void setFirstOffsetTimePretty(String firstOffsetTimePretty) {
        this.firstOffsetTimePretty = firstOffsetTimePretty;
    }

    public long getLastOffsetTime() {
        return lastOffsetTime;
    }

    public void setLastOffsetTime(long lastOffsetTime) {
        this.lastOffsetTime = lastOffsetTime;
    }

    public String getLastOffsetTimePretty() {
        return lastOffsetTimePretty;
    }

    public void setLastOffsetTimePretty(String lastOffsetTimePretty) {
        this.lastOffsetTimePretty = lastOffsetTimePretty;
    }

    @Override
    public String toString() {
        return "KafkaTopicMonitor{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", firstOffsetTime=" + firstOffsetTime +
                ", firstOffsetTimePretty='" + firstOffsetTimePretty + '\'' +
                ", lastOffsetTime=" + lastOffsetTime +
                ", lastOffsetTimePretty='" + lastOffsetTimePretty + '\'' +
                ", logSize=" + logSize +
                '}';
    }
}

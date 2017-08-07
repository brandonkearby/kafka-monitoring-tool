package com.symantec.cpe.analytics.core.kafka;

import org.joda.time.DateTime;

/**
 * @author Brandon Kearby
 *         February 21 2017.
 */
public class KafkaTopicMonitor {
    private String topic;
    private Integer partition;
    private Long firstOffset;
    private Long lastOffset;
    private Long firstOffsetTime;
    private String firstOffsetTimePretty;
    private Long lastOffsetTime;
    private String lastOffsetTimePretty;
    private Long logSize;

    public KafkaTopicMonitor(String topic, Integer partition, Long firstOffset, Long firstOffsetTime, Long lastOffset, Long lastOffsetTime, Long logSize) {
        this.topic = topic;
        this.partition = partition;
        this.firstOffset = firstOffset;
        this.firstOffsetTime = firstOffsetTime;
        this.lastOffset = lastOffset;
        this.lastOffsetTime = lastOffsetTime;
        this.logSize = logSize;
        this.firstOffsetTimePretty = firstOffsetTime != null ? new DateTime(this.firstOffsetTime).toString() : "unknown";
        this.lastOffsetTimePretty = lastOffsetTime != null ? new DateTime(this.lastOffsetTime).toString() : "unknown";
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

    public Long getFirstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(Long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public Long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(Long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public Long getLogSize() {
        return logSize;
    }

    public void setLogSize(Long logSize) {
        this.logSize = logSize;
    }

    public Long getFirstOffsetTime() {
        return firstOffsetTime;
    }

    public void setFirstOffsetTime(Long firstOffsetTime) {
        this.firstOffsetTime = firstOffsetTime;
    }

    public String getFirstOffsetTimePretty() {
        return firstOffsetTimePretty;
    }

    public void setFirstOffsetTimePretty(String firstOffsetTimePretty) {
        this.firstOffsetTimePretty = firstOffsetTimePretty;
    }

    public Long getLastOffsetTime() {
        return lastOffsetTime;
    }

    public void setLastOffsetTime(Long lastOffsetTime) {
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

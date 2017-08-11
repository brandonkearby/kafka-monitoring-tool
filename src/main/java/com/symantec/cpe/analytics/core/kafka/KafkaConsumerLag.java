package com.symantec.cpe.analytics.core.kafka;

public class KafkaOffsetMonitor {
    String consumerGroupName;
    String topic;
    Integer partition;
    long logSize;
    long consumerOffset;
    long lag;

    public KafkaOffsetMonitor(String consumerGroupName, String topic, Integer partition,
                              long logSize, long consumerOffset, long lag) {
        this.consumerGroupName = consumerGroupName;
        this.topic = topic;
        this.partition = partition;
        this.logSize = logSize;
        this.consumerOffset = consumerOffset;
        this.lag = lag;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
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

    public long getLogSize() {
        return logSize;
    }

    public void setLogSize(long logSize) {
        this.logSize = logSize;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }
}

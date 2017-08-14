package com.symantec.cpe.analytics.core.kafka;

import java.io.Serializable;

public class KafkaConsumerLag implements Serializable {
    private String consumerGroupName;
    private String topic;
    private long lag;

    public KafkaConsumerLag(String consumerGroupName, String topic) {
        this.consumerGroupName = consumerGroupName;
        this.topic = topic;
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

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaConsumerLag that = (KafkaConsumerLag) o;

        if (!consumerGroupName.equals(that.consumerGroupName)) return false;
        return topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        int result = consumerGroupName.hashCode();
        result = 31 * result + topic.hashCode();
        return result;
    }
}

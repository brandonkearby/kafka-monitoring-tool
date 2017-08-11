package com.symantec.cpe.analytics.core.kafka;

import java.util.List;

public class KafkaConsumerLag {
    String consumerGroupName;
    String topic;
    long lag;

    public KafkaConsumerLag(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
        for (KafkaOffsetMonitor kafkaOffsetMonitor : kafkaOffsetMonitors) {
            consumerGroupName = kafkaOffsetMonitor.consumerGroupName;
            topic = kafkaOffsetMonitor.topic;
            lag += kafkaOffsetMonitor.lag;
        }
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
}

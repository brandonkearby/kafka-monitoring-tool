package com.symantec.cpe.analytics.kafka;

import kafka.common.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class ConsumerGroupState {
    private ConsumerGroup consumerGroup;

    private Map<Topic, Map<Partition, OffsetState>> topicAndPartitionState = new LinkedHashMap<>();

    public ConsumerGroupState(ConsumerGroup consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public ConsumerGroup getConsumerGroup() {
        return consumerGroup;
    }

    public Set<Topic> getTopics() {
        return new HashSet<>(topicAndPartitionState.keySet());
    }

    public Map<Partition, OffsetState> getPartitionOffsetState(Topic topic) {
        return topicAndPartitionState.get(topic);
    }

    public void set(TopicPartition topicPartition, OffsetAndMetadata consumerOffset) {
        Topic topic = new Topic(topicPartition.topic());
        Map<Partition, OffsetState> consumerTopicAndPartitionStates = topicAndPartitionState.get(topic);
        if (consumerTopicAndPartitionStates == null) {
            consumerTopicAndPartitionStates = new TreeMap<>();
            topicAndPartitionState.put(topic, consumerTopicAndPartitionStates);
        }
        Partition partition = new Partition(topicPartition.partition());
        consumerTopicAndPartitionStates.put(partition, new OffsetState(consumerOffset));
    }

    @Override
    public String toString() {
        return "ConsumerGroupState{" +
                "consumerGroup=" + consumerGroup +
                ", topicAndPartitionState=" + topicAndPartitionState +
                '}';
    }

}


package com.symantec.cpe.analytics.kafka;

import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.kafka.KafkaTopicMonitor;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.coordinator.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class ClusterMonitorRunnable implements Runnable {

    private static final String CONSUMER_OFFSETS_TOPIC = "__consumer_offsets";
    private static final byte[] lock = new byte[0];
    public static final long POLL_TIMEOUT = 10L;

    private boolean running;
    private KafkaMonitorConfiguration kafkaMonitorConfiguration;
    private KafkaConsumer<ByteBuffer, ByteBuffer> consumer;
    private ClusterState clusterState;
    private static final Logger log = LoggerFactory.getLogger(ClusterMonitorRunnable.class);


    public ClusterMonitorRunnable(KafkaMonitorConfiguration kafkaMonitorConfiguration, ClusterState clusterState) {
        Objects.requireNonNull(kafkaMonitorConfiguration, "KafkaMonitorConfiguration can't be null");
        Objects.requireNonNull(clusterState, "ClusterState can't be null");
        this.kafkaMonitorConfiguration = kafkaMonitorConfiguration;
        this.clusterState = clusterState;
    }


    public void run() {
        running = true;
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMonitorConfiguration.getBootstrapServer());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaMonitorConfiguration.MONITORING_KAFKA_GROUP);
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(configs, new ByteBufferDeserializer(), new ByteBufferDeserializer());
        consumer.subscribe(Collections.singletonList(CONSUMER_OFFSETS_TOPIC));

        init(consumer);

        while (running) {
            ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(Integer.MAX_VALUE);
            Iterable<ConsumerRecord<ByteBuffer, ByteBuffer>> iterable = records.records(CONSUMER_OFFSETS_TOPIC);
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : iterable) {
                BaseKey baseKey = GroupMetadataManager.readMessageKey(record.key());
                if (baseKey instanceof GroupMetadataKey) {
                    GroupMetadataKey groupMetadataKey = (GroupMetadataKey) baseKey;
                    GroupMetadata groupMetadata = GroupMetadataManager.readGroupMessageValue(baseKey.toString(), record.value());
                    if (groupMetadata != null && !KafkaMonitorConfiguration.MONITORING_KAFKA_GROUP.equals(groupMetadata.groupId())) {
                        //todo handle manual removal of consumer groups
                        log.debug("groupMetadataKey = " + groupMetadataKey);
                        log.debug("groupMetadata = " + groupMetadata);
                    }
                } else if (baseKey instanceof OffsetKey) {
                    OffsetKey offsetKey = (OffsetKey) baseKey;
                    GroupTopicPartition groupTopicPartition = offsetKey.key();
                    if (KafkaMonitorConfiguration.MONITORING_KAFKA_GROUP.equals(groupTopicPartition.group())) {
                        continue;
                    }

                    log.debug("offsetKey = " + offsetKey);
                    OffsetAndMetadata offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(record.value());


                    ConsumerGroup consumerGroup = new ConsumerGroup(groupTopicPartition.group());
                    ConsumerGroupState consumerGroupState = clusterState.get(consumerGroup);
                    if (consumerGroupState == null) {
                        consumerGroupState = new ConsumerGroupState(consumerGroup);
                        clusterState.setConsumerGroupState(consumerGroup, consumerGroupState);
                    }
                    if (offsetAndMetadata == null) {
                        offsetAndMetadata = new OffsetAndMetadata(new OffsetMetadata(getFirstOffset(groupTopicPartition.topicPartition()), ""), 0,0);
                    }
                    log.debug("offsetAndMetadata = " + offsetAndMetadata);
                    log.debug("consumerGroupState = " + consumerGroupState);

                    consumerGroupState.set(groupTopicPartition.topicPartition(), offsetAndMetadata);

                } else {
                    throw new IllegalStateException("Unknown BaseKey: " + baseKey);
                }
            }

        }


    }



    private void init(KafkaConsumer consumer) {
        while (true) {
            consumer.poll(0);
            Set<TopicPartition> assignment = consumer.assignment();
            long dateTime = DateTime.now().minusHours(5).getMillis();
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition topicPartition : assignment) {
                timestampsToSearch.put(topicPartition, dateTime);
            }

            if (assignment.isEmpty()) {
                reallySleep();
            } else {
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
                for (TopicPartition topicPartition : assignment) {
                    OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(topicPartition);
                    if (offsetAndTimestamp != null) {
                        consumer.seek(topicPartition, offsetAndTimestamp.offset());
                        log.info("Seeking 5 hrs back for: " + topicPartition);
                    }
                    else {
                        consumer.seekToBeginning(Collections.singleton(topicPartition));
                        log.info("Seeking to beginning for: " + topicPartition);
                    }
                }
                break;
            }
        }
    }

    private void reallySleep() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long getLastOffset(TopicPartition topicPartition) {
        synchronized (lock) {
            KafkaConsumer consumer = getLatestOffsetConsumer();
            Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);
            consumer.assign(topicPartitionSet);
            consumer.seekToEnd(topicPartitionSet);
            long lastOffset = consumer.position(topicPartition);
            consumer.unsubscribe();
            return lastOffset;
        }
    }

    private long getFirstOffset(TopicPartition topicPartition) {
        synchronized (lock) {
            KafkaConsumer consumer = getLatestOffsetConsumer();
            Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);
            consumer.assign(topicPartitionSet);
            consumer.seekToBeginning(topicPartitionSet);
            long lastOffset = consumer.position(topicPartition);
            consumer.unsubscribe();
            return lastOffset;
        }
    }

    private Long getFirstOffsetTime(TopicPartition topicPartition) {
        synchronized (lock) {
            KafkaConsumer consumer = getLatestOffsetConsumer();
            Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);
            consumer.assign(topicPartitionSet);
            consumer.seekToBeginning(topicPartitionSet);
            ConsumerRecords consumerRecords = consumer.poll(POLL_TIMEOUT);
            if (consumerRecords.isEmpty()) {
                return null;
            }
            List records = consumerRecords.records(topicPartition);
            long timestamp = ((ConsumerRecord)records.get(0)).timestamp();
            consumer.unsubscribe();
            return timestamp;
        }
    }
    private Long getLastOffsetTime(TopicPartition topicPartition) {
        long lastCommitedOffset = Math.max(getLastOffset(topicPartition) - 1, 0);
        synchronized (lock) {
            KafkaConsumer consumer = getLatestOffsetConsumer();
            Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);
            consumer.assign(topicPartitionSet);
            consumer.seek(topicPartition, lastCommitedOffset);
            ConsumerRecords consumerRecords = consumer.poll(POLL_TIMEOUT);
            if (consumerRecords.isEmpty()) {
                return null;
            }
            List records = consumerRecords.records(topicPartition);
            long timestamp = ((ConsumerRecord)records.get(0)).timestamp();
            consumer.unsubscribe();
            return timestamp;
        }
    }

    private OffsetAndTimestamp getOffsetAtTime(TopicPartition topicPartition, long timeSinceEpoc) {
        synchronized (lock) {
            KafkaConsumer consumer = getLatestOffsetConsumer();
            Map<TopicPartition, Long> topicPartitionTimeMap = new HashMap<>();
            topicPartitionTimeMap.put(topicPartition, timeSinceEpoc);
            Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(topicPartitionTimeMap);
            if (map.isEmpty()) {
                throw new IllegalStateException("Unable to locate offset for time: " + timeSinceEpoc + " for topic/partition: " + topicPartition.toString());
            }
            consumer.unsubscribe();
            return map.values().iterator().next();
        }
    }

    private KafkaConsumer<String, String> latestOffsetConsumer;
    private KafkaConsumer getLatestOffsetConsumer() {
        if (latestOffsetConsumer == null) {
            Properties props = getProperties(KafkaMonitorConfiguration.MONITORING_KAFKA_GROUP);
            latestOffsetConsumer = new KafkaConsumer<>(props);
            log.info("Created a new Kafka Consumer");
        }
        return latestOffsetConsumer;
    }

    private Properties getProperties(String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMonitorConfiguration.getBootstrapServer());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        return props;
    }

    public List<KafkaOffsetMonitor> getKafkaOffsetMonitors() {
        List<KafkaOffsetMonitor> kafkaOffsetMonitors = new ArrayList<>();
        Set<ConsumerGroup> consumerGroups = this.clusterState.getConsumerGroups();
        for (ConsumerGroup consumerGroup : consumerGroups) {
            ConsumerGroupState consumerGroupState = this.clusterState.get(consumerGroup);
            Set<Topic> topics = consumerGroupState.getTopics();
            for (Topic topic : topics) {
                Map<Partition, OffsetState> partitionOffsetState = consumerGroupState.getPartitionOffsetState(topic);
                for (Map.Entry<Partition, OffsetState> entry : partitionOffsetState.entrySet()) {
                    OffsetState offsetState = entry.getValue();
                    long lastOffset = getLastOffset(new TopicPartition(topic.getName(), entry.getKey().id));
                    long consumerOffset = offsetState.getConsumerOffsetMetadata().offset();
                    long lag = lastOffset - consumerOffset;

                    KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor(consumerGroup.groupId,
                            topic.getName(), entry.getKey().id, lastOffset, consumerOffset, lag);
                    kafkaOffsetMonitors.add(kafkaOffsetMonitor);
                }
            }
        }
        return kafkaOffsetMonitors;
    }

    public void stop() {
        this.running = false;
    }

    private void refreshTopicState() {
        synchronized (lock) {
            KafkaConsumer latestOffsetConsumer = getLatestOffsetConsumer();
            Map<String, List<PartitionInfo>> topics = latestOffsetConsumer.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
                List<PartitionInfo> partitionInfos = entry.getValue();
                for (PartitionInfo partitionInfo : partitionInfos) {
                    Topic topic = new Topic(entry.getKey());
                    Partition partition = new Partition(partitionInfo.partition());
                    TopicPartition topicPartition = new TopicPartition(topic.getName(), partition.id);
                    long firstOffset = getFirstOffset(topicPartition);
                    Long firstOffsetTime = getFirstOffsetTime(topicPartition);
                    long lastOffset = getLastOffset(topicPartition);
                    Long lastOffsetTime = getLastOffsetTime(topicPartition);
                    this.clusterState.setTopicState(topic, partition, firstOffset, lastOffset, firstOffsetTime, lastOffsetTime);
                }
            }
        }
    }

    public List<KafkaTopicMonitor> getKafkaTopicMonitors() {
        refreshTopicState();
        List<KafkaTopicMonitor> kafkaTopicMonitors = new ArrayList<>();
        Set<Topic> topics = this.clusterState.getTopics();
        for (Topic topic : topics) {
            if (topic.getName().equals(CONSUMER_OFFSETS_TOPIC)) {
                continue;
            }
            TopicState topicState = this.clusterState.getTopicState(topic);
            if (topicState == null) {
                continue;
            }
            Set<Partition> partitions = topicState.getPartitions();
            for (Partition partition : partitions) {
                Long firstOffset = topicState.getFirstOffset(partition);
                Long firstOffsetTime = topicState.getFirstOffsetTime(partition);
                Long lastOffset = topicState.getLastOffset(partition);
                Long lastOffsetTime = topicState.getLastOffsetTime(partition);
                kafkaTopicMonitors.add(new KafkaTopicMonitor(topic.getName(), partition.id, firstOffset, firstOffsetTime, lastOffset, lastOffsetTime, lastOffset - firstOffset));
            }
        }
        return kafkaTopicMonitors;
    }

    public void seekToBeginning(String consumerGroup, String topic) {
        refreshTopicState();
        TopicState topicState = clusterState.getTopicState(new Topic(topic));
        Set<Partition> partitions = topicState.getPartitions();
        Properties properties = getProperties(consumerGroup);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        for (Partition partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition.id);
            Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToBeginning(topicPartitions);
            kafkaConsumer.position(topicPartition);
            kafkaConsumer.poll(0);
            kafkaConsumer.commitSync();
            kafkaConsumer.unsubscribe();
        }
        kafkaConsumer.close();
    }

    public void seekToEnd(String consumerGroup, String topic) {
        refreshTopicState();
        TopicState topicState = clusterState.getTopicState(new Topic(topic));
        Set<Partition> partitions = topicState.getPartitions();
        Properties properties = getProperties(consumerGroup);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        for (Partition partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition.id);
            Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToEnd(topicPartitions);
            kafkaConsumer.position(topicPartition);
            kafkaConsumer.poll(0);
            kafkaConsumer.commitSync();
            kafkaConsumer.unsubscribe();
        }
        kafkaConsumer.close();
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public List seek(String consumerGroup, String topic, long time) {
        List<Map> statuses = new ArrayList<>();
        refreshTopicState();
        TopicState topicState = clusterState.getTopicState(new Topic(topic));
        Set<Partition> partitions = topicState.getPartitions();
        Properties properties = getProperties(consumerGroup);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        for (Partition partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition.id);
            Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);
            OffsetAndTimestamp offsetAtTime = getOffsetAtTime(topicPartition, time);
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("consumerGroup", consumerGroup);
            status.put("topic", topic);
            status.put("time", time);
            status.put("topicPartition", topicPartition.toString());
            statuses.add(status);
            if (offsetAtTime != null) {
                kafkaConsumer.assign(topicPartitions);
                kafkaConsumer.seek(topicPartition, offsetAtTime.offset());
                long position = kafkaConsumer.position(topicPartition);
                assert offsetAtTime.offset() == position;
                kafkaConsumer.poll(0);
                kafkaConsumer.commitSync();
                kafkaConsumer.unsubscribe();
                status.put("offset", position);
                status.put("status", "success");
            }
            else {
                status.put("status", "failed. Unable to locate offset at that time");
            }
        }
        kafkaConsumer.close();
        return statuses;
    }
}

package com.symantec.cpe.analytics.kafka;

import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.kafka.KafkaTopicMonitor;
import io.dropwizard.lifecycle.Managed;

import java.util.List;
import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class ClusterMonitorService implements Managed {

    private ClusterMonitorRunnable clusterMonitorRunnable;


    public ClusterMonitorService(KafkaMonitorConfiguration kafkaMonitorConfiguration, ClusterState clusterState) {
        Objects.requireNonNull(kafkaMonitorConfiguration, "KafkaMonitorConfiguration can't be null");
        Objects.requireNonNull(clusterState, "ClusterState can't be null");
        clusterMonitorRunnable = new ClusterMonitorRunnable(kafkaMonitorConfiguration, clusterState);
    }

    @Override
    public void stop() throws Exception {
        clusterMonitorRunnable.stop();
    }

    public void start() {
        new Thread(clusterMonitorRunnable).start();
    }

    public List<KafkaOffsetMonitor> getKafkaOffsetMonitors() {
        return clusterMonitorRunnable.getKafkaOffsetMonitors();
    }

    public List<KafkaTopicMonitor> getKafkaTopicMonitors(Topic topic) {
        return clusterMonitorRunnable.getKafkaTopicMonitors(topic);
    }

    public List seekToBeginning(String consumerGroup, String topic) {
        return clusterMonitorRunnable.seekToBeginning(consumerGroup, topic);
    }

    public List seekToEnd(String consumerGroup, String topic) {
        return clusterMonitorRunnable.seekToEnd(consumerGroup, topic);
    }

    public ClusterState getClusterState() {
        return clusterMonitorRunnable.getClusterState();
    }

    public List seek(String consumerGroup, String topic, long timeInMs) {
        return clusterMonitorRunnable.seek(consumerGroup, topic, timeInMs);
    }
}

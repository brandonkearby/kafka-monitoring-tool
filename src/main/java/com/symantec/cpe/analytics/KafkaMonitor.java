package com.symantec.cpe.analytics;

import com.symantec.cpe.analytics.kafka.ClusterMonitorService;
import com.symantec.cpe.analytics.kafka.ClusterState;
import com.symantec.cpe.analytics.resources.kafka.KafkaConsumerResource;
import com.symantec.cpe.analytics.resources.kafka.KafkaResource;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class KafkaMonitor extends Application<KafkaMonitorConfiguration> {

    @Override
    public void initialize(Bootstrap<KafkaMonitorConfiguration> bootstrap) {
    }

    @Override
    public void run(KafkaMonitorConfiguration configuration, Environment environment)
            throws Exception {
        ClusterMonitorService clusterMonitor = new ClusterMonitorService(configuration, new ClusterState());
        environment.lifecycle().manage(clusterMonitor);

        environment.jersey().register(new KafkaResource(clusterMonitor));
        environment.jersey().register(new KafkaConsumerResource(clusterMonitor));
    }

    @Override
    public String getName() {
        return "kafka-monitor";
    }

    public static void main(String[] args) throws Exception {
        new KafkaMonitor().run(args);
    }

}

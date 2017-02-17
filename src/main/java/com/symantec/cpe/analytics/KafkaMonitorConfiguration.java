package com.symantec.cpe.analytics;

import io.dropwizard.Configuration;

import javax.validation.Valid;

public class KafkaMonitorConfiguration extends Configuration {

    public static final String MONITORING_KAFKA_GROUP = "kafka-monitoring";

    @Valid
    private String bootstrapServer = "localhost:9092";

    @Valid
    private int refreshSeconds = 60;

    public int getRefreshSeconds() {
        return refreshSeconds;
    }

    public void setRefreshSeconds(int refreshSeconds) {
        this.refreshSeconds = refreshSeconds;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }
}

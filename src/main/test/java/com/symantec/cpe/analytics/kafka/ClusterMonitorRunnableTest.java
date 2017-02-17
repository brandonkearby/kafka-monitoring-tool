package com.symantec.cpe.analytics.kafka;

import com.symantec.cpe.analytics.KafkaMonitorConfiguration;
import org.junit.Test;

/**
 * @author Brandon Kearby
 *         February 15 2017.
 */
public class ClusterMonitorRunnableTest {

    @Test
    public void test() {
        new ClusterMonitorRunnable(new KafkaMonitorConfiguration(), new ClusterState()).run();
    }
}
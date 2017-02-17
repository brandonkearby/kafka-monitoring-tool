package com.symantec.cpe.analytics.kafka;

import kafka.common.OffsetAndMetadata;

import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 17 2017.
 */
public class OffsetState {
    private OffsetAndMetadata consumerOffsetMetadata;

    public OffsetState(OffsetAndMetadata consumerOffsetMetadata) {
        Objects.requireNonNull(consumerOffsetMetadata, "OffsetAndMetadata can't be null");
        this.consumerOffsetMetadata = consumerOffsetMetadata;
    }

    public OffsetAndMetadata getConsumerOffsetMetadata() {
        return consumerOffsetMetadata;
    }



    @Override
    public String toString() {
        return "OffsetState {" +
                "consumerOffsetMetadata=" + consumerOffsetMetadata + '}';
    }
}

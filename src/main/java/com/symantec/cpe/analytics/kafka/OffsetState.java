package com.symantec.cpe.analytics.kafka;

import kafka.common.OffsetAndMetadata;

import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 17 2017.
 */
public class OffsetState {
    private OffsetAndMetadata consumerOffsetMetadata;
    private long producerOffset;

    public OffsetState(OffsetAndMetadata consumerOffsetMetadata, long producerOffset) {
        Objects.requireNonNull(consumerOffsetMetadata, "OffsetAndMetadata can't be null");
        this.consumerOffsetMetadata = consumerOffsetMetadata;
        this.producerOffset = producerOffset;
    }

    public OffsetAndMetadata getConsumerOffsetMetadata() {
        return consumerOffsetMetadata;
    }

    public long getProducerOffset() {
        return producerOffset;
    }

    public long getLag() {
        return Math.abs(producerOffset - consumerOffsetMetadata.offset());
    }

    @Override
    public String toString() {
        return "OffsetState {" +
                "consumerOffsetMetadata=" + consumerOffsetMetadata +
                ", producerOffset=" + producerOffset +
                ", lag=" + getLag() +
                '}';
    }
}

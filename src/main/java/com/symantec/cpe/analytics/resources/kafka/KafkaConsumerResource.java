package com.symantec.cpe.analytics.resources.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.symantec.cpe.analytics.core.ResponseMessage;
import com.symantec.cpe.analytics.core.kafka.KafkaConsumerLag;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.kafka.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/kafka")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaConsumerResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerResource.class);
    public static final String ALL_MARKER = "_all";
    private ClusterMonitorService clusterMonitorService;

    public KafkaConsumerResource(ClusterMonitorService clusterMonitor) {
        this.clusterMonitorService = clusterMonitor;
    }

    private enum SeekTo {
        beginning,
        end
    }

    @Path("/consumer/{consumerGroup}/seek")
    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    public Response seekToBeginning(@PathParam("consumerGroup") String consumerGroup,
                                    @NotNull @QueryParam("to") String to,
                                    @DefaultValue(ALL_MARKER) @QueryParam("topics") String topics) {
        String responseType = MediaType.APPLICATION_JSON;
        try {
            if (org.apache.commons.lang3.StringUtils.isEmpty(to)) {
                throw new IllegalStateException("Missing required request parameter: 'to'. Valid values: " +
                        Joiner.on(", ").join(SeekTo.values()));
            }
            ClusterState clusterState = clusterMonitorService.getClusterState();
            ConsumerGroupState consumerGroupState = clusterState.get(new ConsumerGroup(consumerGroup));
            Set<String> selectedTopics = Sets.newHashSet(StringUtils.split(topics, ','));
            Set<Topic> topicSet;
            if (selectedTopics.size() == 1 && selectedTopics.contains(ALL_MARKER)) {
                topicSet = consumerGroupState.getTopics();
            }
            else {
                topicSet = selectedTopics.stream().map(Topic::new).collect(Collectors.toSet());
            }

            List messages = new ArrayList();
            for (Topic topic : topicSet) {
                if (SeekTo.beginning.name().equalsIgnoreCase(to)) {
                    messages = clusterMonitorService.seekToBeginning(consumerGroup, topic.getName());
                }
                else if (SeekTo.end.name().equalsIgnoreCase(to)) {
                    messages = clusterMonitorService.seekToEnd(consumerGroup, topic.getName());
                }
                else {
                    long timeInMs = Long.parseLong(to);
                    messages = clusterMonitorService.seek(consumerGroup, topic.getName(), timeInMs);
                }
            }

            return Response.status(Response.Status.OK).type(responseType)
                    .entity(messages)
                    .build();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ResponseMessage(e.getLocalizedMessage())).type(responseType).build();
        }
    }



    @Path("/consumer/{consumerGroup}/offsets")
    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    public Response offsets(@PathParam("consumerGroup") @DefaultValue(ALL_MARKER) String consumerGroup,
                            @QueryParam("topics") @DefaultValue(ALL_MARKER)  String topics,
                            @QueryParam("sum") boolean sum) {
        String responseType = MediaType.APPLICATION_JSON;
        try {
            List<KafkaOffsetMonitor> kafkaOffsetMonitors = this.clusterMonitorService.getKafkaOffsetMonitors();
            Stream<KafkaOffsetMonitor> filtered = ALL_MARKER.equals(consumerGroup) ? kafkaOffsetMonitors.stream() :
                    kafkaOffsetMonitors.stream().filter(
                            kafkaOffsetMonitor -> kafkaOffsetMonitor.getConsumerGroupName().equals(consumerGroup)
                    );
            if (!ALL_MARKER.equals(topics)) {
                filtered = filtered.filter(kafkaOffsetMonitor -> kafkaOffsetMonitor.getTopic().equals(topics));
            }
            kafkaOffsetMonitors = filtered.collect(Collectors.toList());
            if (sum) {
                Set<KafkaConsumerLag> consumerLags = getKafkaConsumerLags(kafkaOffsetMonitors);
                return Response.status(Response.Status.OK).type(responseType)
                        .entity(consumerLags)
                        .build();
            }
            else {
                Set<KafkaConsumerLag> consumerLags = getKafkaConsumerLags(kafkaOffsetMonitors);
                return Response.status(Response.Status.OK).type(responseType)
                        .entity(consumerLags)
                        .build();
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ResponseMessage(e.getLocalizedMessage())).type(responseType).build();
        }
    }

    private Set<KafkaConsumerLag> getKafkaConsumerLags(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
        Map<KafkaConsumerLag, List<KafkaOffsetMonitor>> grouped;
        Function<KafkaOffsetMonitor, KafkaConsumerLag> kafkaOffsetMonitorKafkaConsumerLagFunction = kafkaOffsetMonitor -> new KafkaConsumerLag(kafkaOffsetMonitor.getConsumerGroupName(), kafkaOffsetMonitor.getTopic());
        grouped = kafkaOffsetMonitors.stream().collect(Collectors.groupingBy(kafkaOffsetMonitorKafkaConsumerLagFunction));
        for (Map.Entry<KafkaConsumerLag, List<KafkaOffsetMonitor>> entry : grouped.entrySet()) {
            long lag = entry.getValue().stream().mapToLong(KafkaOffsetMonitor::getLag).sum();
            entry.getKey().setLag(lag);
        }
        return grouped.keySet();
    }

}

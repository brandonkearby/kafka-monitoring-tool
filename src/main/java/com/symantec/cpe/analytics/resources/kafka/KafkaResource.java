package com.symantec.cpe.analytics.resources.kafka;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.symantec.cpe.analytics.core.ResponseMessage;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.kafka.KafkaTopicMonitor;
import com.symantec.cpe.analytics.kafka.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Path("/kafka")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);
    public static final Function<String, Topic> TOPIC_FUNCTION = new Function<String, Topic>() {
        @Nullable
        @Override
        public Topic apply(@Nullable String input) {
            return new Topic(input);
        }
    };
    private ClusterMonitorService clusterMonitorService;

    public KafkaResource(ClusterMonitorService clusterMonitor) {
        this.clusterMonitorService = clusterMonitor;
    }

    @Path("/offset")
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
    public Response getKafkaConsumerOffset(@DefaultValue("json") @QueryParam("outputType") String outputType) {
        String output;
        String responseType = MediaType.APPLICATION_JSON;
        try {
            List<KafkaOffsetMonitor> kafkaOffsetMonitors = clusterMonitorService.getKafkaOffsetMonitors();
            if (outputType.equals("html")) {
                responseType = MediaType.TEXT_HTML;
                output = KafkaConsumerOffsetFormatter.htmlOutput(kafkaOffsetMonitors);
            } else {
                output = KafkaConsumerOffsetFormatter.jsonOutput(kafkaOffsetMonitors);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
        }
        return Response.status(Response.Status.OK).entity(output).type(responseType).build();
    }

    @Path("/topics")
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response getKafkaTopics() {
        String responseType = MediaType.APPLICATION_JSON;
        try {
            List<KafkaTopicMonitor> kafkaOffsetMonitors;
            kafkaOffsetMonitors = clusterMonitorService.getKafkaTopicMonitors();
            return Response.status(Response.Status.OK).entity(kafkaOffsetMonitors).type(responseType).build();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
        }
    }

    private enum SeekTo {
        beginning,
        end
    }
    /**
     * The interval the date histogram is based on.
     */
    static class Interval {

        public static final Interval SECOND = new Interval("1s");
        public static final Interval MINUTE = new Interval("1m");
        public static final Interval HOUR = new Interval("1h");
        public static final Interval DAY = new Interval("1d");
        public static final Interval WEEK = new Interval("1w");
        public static final Interval MONTH = new Interval("1M");
        public static final Interval QUARTER = new Interval("1q");
        public static final Interval YEAR = new Interval("1y");

        public static Interval seconds(int sec) {
            return new Interval(sec + "s");
        }

        public static Interval minutes(int min) {
            return new Interval(min + "m");
        }

        public static Interval hours(int hours) {
            return new Interval(hours + "h");
        }

        public static Interval days(int days) {
            return new Interval(days + "d");
        }

        public static Interval weeks(int weeks) {
            return new Interval(weeks + "w");
        }

        private final String expression;

        public Interval(String expression) {
            this.expression = expression;
        }

        @Override
        public String toString() {
            return expression;
        }
    }

    @Path("/consumer/{consumerGroup}/seek")
    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    public Response seekToBeginning(@PathParam("consumerGroup") String consumerGroup, @NotNull @QueryParam("to") String to, @DefaultValue("_all") @QueryParam("topics") String topics) {
        String responseType = MediaType.APPLICATION_JSON;
        try {
            if (org.apache.commons.lang3.StringUtils.isEmpty(to)) {
                throw new IllegalStateException("Missing required request parameter: 'to'. Valid values: " + Joiner.on(", ").join(SeekTo.values()));
            }
            ClusterState clusterState = clusterMonitorService.getClusterState();
            ConsumerGroupState consumerGroupState = clusterState.get(new ConsumerGroup(consumerGroup));
            String[] selectedTopics = StringUtils.split(topics, ',');
            Set<Topic> topicSet;
            if (selectedTopics.length == 1 && selectedTopics[0].equals("_all")) {
                topicSet = consumerGroupState.getTopics();
            }
            else {
                topicSet = Sets.newHashSet(Lists.transform(Arrays.asList(selectedTopics), TOPIC_FUNCTION));
            }

            for (Topic topic : topicSet) {
                if (SeekTo.beginning.name().equalsIgnoreCase(to)) {
                    clusterMonitorService.seekToBeginning(consumerGroup, topic.getName());
                }
                else if (SeekTo.end.name().equalsIgnoreCase(to)) {
                    clusterMonitorService.seekToEnd(consumerGroup, topic.getName());
                }
                else {
                    throw new UnsupportedOperationException("unable to seek to: " + to);
                    // todo support date math seeking: e.g.: -1hr, -30s, -10m, ...
                }
            }

            return Response.status(Response.Status.OK).type(responseType).build();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ResponseMessage(e.getLocalizedMessage())).type(responseType).build();
        }
    }

}

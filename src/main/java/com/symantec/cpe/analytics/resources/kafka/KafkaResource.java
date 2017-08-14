package com.symantec.cpe.analytics.resources.kafka;

import com.symantec.cpe.analytics.core.ResponseMessage;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import com.symantec.cpe.analytics.core.kafka.KafkaTopicMonitor;
import com.symantec.cpe.analytics.kafka.ClusterMonitorService;
import com.symantec.cpe.analytics.kafka.KafkaConsumerOffsetFormatter;
import com.symantec.cpe.analytics.kafka.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/kafka")
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);
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

    @Path("/topics/{topic}")
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response getKafkaTopics(@PathParam("topic") @DefaultValue("_all") String topic) {
        String responseType = MediaType.APPLICATION_JSON;
        try {
            List<KafkaTopicMonitor> kafkaOffsetMonitors;
            kafkaOffsetMonitors = clusterMonitorService.getKafkaTopicMonitors(new Topic(topic));

            return Response.status(Response.Status.OK).entity(kafkaOffsetMonitors).type(responseType).build();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ResponseMessage("Error Occurred during processing")).type(responseType).build();
        }
    }

}

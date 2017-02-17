package com.symantec.cpe.analytics.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.symantec.cpe.analytics.core.kafka.KafkaOffsetMonitor;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


public class KafkaConsumerOffsetFormatter {

    public static String htmlOutput(List<KafkaOffsetMonitor> kafkaOffsetMonitors) {
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body><pre>");
        sb.append(String.format("%s \t %s \t %s \t %s \t %s \t %s \n",
                StringUtils.rightPad("Consumer Group", 40),
                StringUtils.rightPad("Topic", 40),
                StringUtils.rightPad("Partition", 10),
                StringUtils.rightPad("Log Size", 10),
                StringUtils.rightPad("Consumer Offset", 15),
                StringUtils.rightPad("Lag", 10)));
        for (KafkaOffsetMonitor kafkaOffsetMonitor : kafkaOffsetMonitors) {
            sb.append(String.format("%s \t %s \t %s \t %s \t %s \t %s \n",
                    StringUtils.rightPad(kafkaOffsetMonitor.getConsumerGroupName(), 40),
                    StringUtils.rightPad(kafkaOffsetMonitor.getTopic(), 40),
                    StringUtils.rightPad("" + kafkaOffsetMonitor.getPartition(), 10),
                    StringUtils.rightPad("" + kafkaOffsetMonitor.getLogSize(), 10),
                    StringUtils.rightPad("" + kafkaOffsetMonitor.getConsumerOffset(), 15),
                    StringUtils.rightPad("" + kafkaOffsetMonitor.getLag(), 10)));
        }
        sb.append("</pre></body></html>");
        return sb.toString();
    }


    public static String jsonOutput(List<KafkaOffsetMonitor> kafkaOffsetMonitors) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(kafkaOffsetMonitors);
    }
}

package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Service
public class SQSRawDataListenerService {
	
	private static final Logger logger = LoggerFactory.getLogger(SQSRawDataListenerService.class);
	
	private final SQSRuleRequestListenerService requestListenerService;
    private final ObjectMapper objectMapper;

  
    @SqsListener(value = "${aws.sqs.queue.workflow.ingestion.url}", factory = "workflowSqsFactory")
    public void handleRawDataMessage(String message) {
    	logger.info( "Received Raw Data Message from SQS");

        try {
            Map<String, Object> payload = objectMapper.readValue(
                    message, new TypeReference<Map<String, Object>>() {}
            );

            // Validate and forward to rules request queue
            requestListenerService.forwardToRulesRequestQueue(payload);

        } catch (Exception e) {
        	logger.error("Failed to process Raw Data Message", e);
        }
    }
}

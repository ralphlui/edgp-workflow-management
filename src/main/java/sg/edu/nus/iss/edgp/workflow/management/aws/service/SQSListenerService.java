package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.awspring.cloud.sqs.annotation.SqsListener;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SQSListenerService {

	 private final ObjectMapper objectMapper = new ObjectMapper();
		private static final Logger logger = LoggerFactory.getLogger(SQSListenerService.class);
		
		@SqsListener(value = "${aws.sqs.queue.rule.response.url}")
		public void handleRuleServiceSqsMessage(String message) {
	        try {
	            logger.info("Received Rule Response Message from SQS");
	            
	            try {
	                Map<String, Object> dynamicJson = objectMapper.readValue(message, Map.class);
	                logger.info("Message parsed successfully in receive rule response SqsListener {}", dynamicJson);
	            } catch (Exception e) {
	                logger.error("Error parsing message from rule response SQS: Invalid format or structure", e);
	                return; 
	            }
	            
	        } catch (Exception e) {
	            logger.error("Error processing message from rule response SQS", e);
	        }
	    }
}

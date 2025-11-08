package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.type.TypeReference;

@Service
@RequiredArgsConstructor
public class SQSRuleResponseListenerService {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final WorkflowService workflowService;
	private static final Logger logger = LoggerFactory.getLogger(SQSRuleResponseListenerService.class);

	@SqsListener(value = "${aws.sqs.queue.workflow.rule.response.url}", factory = "workflowSqsFactory")
	public void handleRuleServiceSqsMessage(String message) {
		try {
			logger.info("Received Rule Response Message from SQS");

			try {
				Map<String, Object> dynamicJson = objectMapper.readValue(message,
						new TypeReference<Map<String, Object>>() {
						});
				logger.info("Message parsed successfully in receive rule response SqsListener.");
				workflowService.updateRuleWorkflowStatus(dynamicJson);
			} catch (Exception e) {
				logger.error("Error parsing message from rule response SQS: Invalid format or structure", e);
				return;
			}

		} catch (Exception e) {
			logger.error("Error processing message from rule response SQS", e);
		}
	}
}

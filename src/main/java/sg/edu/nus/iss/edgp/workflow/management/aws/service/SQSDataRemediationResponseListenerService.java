package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DataRemediationService;

@Service
@RequiredArgsConstructor
public class SQSDataRemediationResponseListenerService {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final DataRemediationService dataRemediationService;
	private static final Logger logger = LoggerFactory.getLogger(SQSRuleResponseListenerService.class);

	@SqsListener(value = "${aws.sqs.queue.workflow.dataremediation.response.url}", factory = "workflowSqsFactory")
	public void handleRuleServiceSqsMessage(String message) {
		try {
			logger.info("Received Rule Response Message from SQS");

			try {
				Map<String, Object> dynamicJson = objectMapper.readValue(message,
						new TypeReference<Map<String, Object>>() {
						});
				logger.info("Message parsed successfully in receive data remediation response SqsListener.");
				dataRemediationService.updateDataRemediationResponse(dynamicJson);
			} catch (Exception e) {
				logger.error("Error parsing message from data remediation response SQS: Invalid format or structure",
						e);
				return;
			}

		} catch (Exception e) {
			logger.error("Error processing message from data remediation response SQS", e);
		}
	}
}

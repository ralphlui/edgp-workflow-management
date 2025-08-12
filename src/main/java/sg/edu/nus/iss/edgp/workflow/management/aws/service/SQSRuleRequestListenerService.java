package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@RequiredArgsConstructor
@Service
public class SQSRuleRequestListenerService {

	private static final Logger logger = LoggerFactory.getLogger(SQSRuleRequestListenerService.class);

	private final SqsAsyncClient sqsAsyncClient;
	private final ObjectMapper objectMapper;

	@Value("${aws.sqs.queue.workflow.rule.request.ur}")
	private String ruleRequestQueueUrl;

	public void forwardToRulesRequestQueue(Map<String, Object> payload) {
		try {
			if (!payload.containsKey("data_entry")) {
				logger.warn("Payload missing required field 'data_entry'");
				return;
			}

			String message = objectMapper.writeValueAsString(payload);

			sqsAsyncClient
					.sendMessage(
							SendMessageRequest.builder().queueUrl(ruleRequestQueueUrl).messageBody(message).build())
					.thenAccept(resp -> logger.info("Forwarded to Rules Request Queue, messageId={}", resp.messageId()))
					.exceptionally(ex -> {
						logger.error("Failed to forward message to Rules Request Queue", ex);
						return null;
					});

		} catch (Exception e) {
			logger.error("Failed to serialize/submit message to Rules Request Queue", e);
		}
	}

}

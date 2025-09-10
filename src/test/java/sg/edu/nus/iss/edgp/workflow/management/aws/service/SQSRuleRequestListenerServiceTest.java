package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@ExtendWith(MockitoExtension.class)
class SQSRuleRequestListenerServiceTest {

	private SqsAsyncClient sqsAsyncClient;
	private ObjectMapper objectMapper;
	private SQSRuleRequestListenerService service;

	@BeforeEach
	void setUp() {
		sqsAsyncClient = mock(SqsAsyncClient.class);
		objectMapper = mock(ObjectMapper.class);
		service = new SQSRuleRequestListenerService(

				sqsAsyncClient, objectMapper);

		ReflectionTestUtils.setField(service, "ruleRequestQueueUrl", "https://sqs.test/queue");
	}

	@Test
	void rule_forward_sends_whenDataEntryPresent() throws Exception {

		Map<String, Object> payload = new HashMap<>();
		payload.put("data_entry", Map.of("id", "1"));
		when(objectMapper.writeValueAsString(payload)).thenReturn("{\"data_entry\":{\"id\":\"1\"}}");

		SendMessageResponse resp = SendMessageResponse.builder().messageId("mid-123").build();
		when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class)))
				.thenReturn(CompletableFuture.completedFuture(resp));

		service.forwardToRulesRequestQueue(payload);

		ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(sqsAsyncClient, times(1)).sendMessage(cap.capture());
		SendMessageRequest req = cap.getValue();
		org.junit.jupiter.api.Assertions.assertEquals("https://sqs.test/queue", req.queueUrl());
		org.junit.jupiter.api.Assertions.assertEquals("{\"data_entry\":{\"id\":\"1\"}}", req.messageBody());

		verify(objectMapper, times(1)).writeValueAsString(payload);
	}

	@Test
	void rule_forward_whenDataEntryMissing() throws Exception {

		Map<String, Object> payload = Map.of("foo", "bar");

		service.forwardToRulesRequestQueue(payload);

		verifyNoInteractions(objectMapper);
		verifyNoInteractions(sqsAsyncClient);
	}

	@Test
	void rule_forward_handlesError() throws Exception {

		Map<String, Object> payload = Map.of("data_entry", Map.of("id", "1"));
		when(objectMapper.writeValueAsString(payload)).thenThrow(new JsonProcessingException("boom") {
		});

		service.forwardToRulesRequestQueue(payload);

		verify(objectMapper, times(1)).writeValueAsString(payload);
		verifyNoInteractions(sqsAsyncClient);
	}

	@Test
	void rule_forward_handlesAsyncFailure() throws Exception {

		Map<String, Object> payload = Map.of("data_entry", Map.of("id", "1"));
		when(objectMapper.writeValueAsString(payload)).thenReturn("{\"data_entry\":{\"id\":\"1\"}}");

		CompletableFuture<SendMessageResponse> failed = new CompletableFuture<>();
		failed.completeExceptionally(new RuntimeException("SQS down"));
		when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(failed);

		service.forwardToRulesRequestQueue(payload);

		verify(sqsAsyncClient, times(1)).sendMessage(any(SendMessageRequest.class));
		verify(objectMapper, times(1)).writeValueAsString(payload);
	}
}

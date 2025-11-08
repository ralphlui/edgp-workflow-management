package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;

import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@ExtendWith(MockitoExtension.class)
class SQSPublishingServiceTest {

	private SqsClient sqsClient;
	private SQSPublishingService service;

	@BeforeEach
	void setUp() {
		sqsClient = mock(SqsClient.class);

		service = new SQSPublishingService(sqsClient);
		ReflectionTestUtils.setField(service, "sqsClient", sqsClient);
		ReflectionTestUtils.setField(service, "auditQueueURL", "https://sqs.test/audit-queue");
	}

	@Test
	void sendMessage_smallPayload_sentWithoutTruncation() {
		// Arrange
		AuditDTO dto = new AuditDTO();
		dto.setUserId("id-1");
		dto.setRemarks("OK");

		when(sqsClient.sendMessage(any(SendMessageRequest.class)))
				.thenReturn(SendMessageResponse.builder().messageId("m-1").build());

		// Act
		service.sendMessage(dto);

		// Assert
		ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(sqsClient, times(1)).sendMessage(cap.capture());
		SendMessageRequest req = cap.getValue();

		assertEquals("https://sqs.test/audit-queue", req.queueUrl());
		assertEquals(5, req.delaySeconds());

		String body = req.messageBody();
		assertTrue(body.contains("\"remarks\":\"OK\""));

	}

	@Test
	void sendMessage_oversizePayload_truncatesAndSends() throws JsonMappingException, JsonProcessingException {
		String bigRemarks = "A".repeat(350_000);
		AuditDTO dto = new AuditDTO();
		dto.setUserId("id-oversize");
		dto.setRemarks(bigRemarks);

		when(sqsClient.sendMessage(any(SendMessageRequest.class)))
				.thenReturn(SendMessageResponse.builder().messageId("m-oversize").build());

		service.sendMessage(dto);

		ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(sqsClient).sendMessage(cap.capture());

		String body = cap.getValue().messageBody();
		assertTrue(body.getBytes(StandardCharsets.UTF_8).length <= 256 * 1024);

		var node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(body);
		assertTrue(node.path("remarks").asText().endsWith("..."));
	}

	@Test
	void sendMessage_sqsThrows_isCaught() {

		AuditDTO dto = new AuditDTO();
		dto.setUserId("id-err");
		dto.setRemarks("Error");
		when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenThrow(new RuntimeException("SQS down"));

		assertDoesNotThrow(() -> service.sendMessage(dto));
		verify(sqsClient, times(1)).sendMessage(any(SendMessageRequest.class));
	}

	@Test
	void truncateMessage_diffExceedsRemarkSize_returnsEmpty() throws Exception {

		String remarks = "12345";
		String currentMessage = "X".repeat((256 * 1024) + 10);

		String out = service.truncateMessage(remarks, 256 * 1024, currentMessage);

		assertEquals("", out);
	}

	@Test
	void truncateMessage_allowedBytesGreaterThanRemark_keepsOriginal() {

		String remarks = "hello";
		String currentMessage = "X".repeat((256 * 1024) - 100);

		String out = service.truncateMessage(remarks, 256 * 1024, currentMessage);

		assertEquals("hello", out);
	}

	@Test
	void truncateMessage_truncatesToAllowedBytes() {

		String remarks = "abcdefghij";
		int max = 1024;

		String currentMessage = "X".repeat(max + 1);

		String out = service.truncateMessage(remarks, max, currentMessage);

		assertEquals("abcd", out);
	}

	@Test
	void truncateMessage_onException_returnsOriginal() {

		SQSPublishingService spyService = spy(service);
		String remarks = "xyz";
		String currentMessage = "x";
		assertDoesNotThrow(() -> spyService.truncateMessage(remarks, 1, currentMessage));
	}
}

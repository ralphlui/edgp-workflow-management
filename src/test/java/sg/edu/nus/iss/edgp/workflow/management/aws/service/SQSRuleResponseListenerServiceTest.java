package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

@ExtendWith(MockitoExtension.class)
class SQSRuleResponseListenerServiceTest {

	@Mock
	private WorkflowService workflowService;

	@InjectMocks
	private SQSRuleResponseListenerService listener;

	private ObjectMapper mapper;

	@BeforeEach
	void setUp() {
		mapper = new ObjectMapper();
	}

	@Test
	@DisplayName("Happy path: valid JSON message parsed and passed to workflowService.updateWorkflowStatus")
	void handleRuleServiceSqsMessage_validJson() throws Exception {
		// Arrange
		String json = "{\"status\":\"SUCCESS\",\"data\":{\"id\":\"wf-123\"}}";

		// Act
		listener.handleRuleServiceSqsMessage(json);

		// Assert
		ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
		verify(workflowService).updateRuleWorkflowStatus(captor.capture());

		Map<String, Object> parsed = captor.getValue();
		// contains status and data map
		assertEquals("SUCCESS", parsed.get("status"));
		Map<?, ?> data = (Map<?, ?>) parsed.get("data");
		assertEquals("wf-123", data.get("id"));
	}

	@Test
	@DisplayName("Invalid JSON: parsing fails, workflowService not called")
	void handleRuleServiceSqsMessage_invalidJson() {
		// Arrange
		String invalidJson = "{not-valid-json";

		// Act
		listener.handleRuleServiceSqsMessage(invalidJson);

		// Assert
		verifyNoInteractions(workflowService);
	}

	@Test
	@DisplayName("ObjectMapper throws exception unexpectedly → caught by outer try")
	void handleRuleServiceSqsMessage_objectMapperThrows() throws Exception {
		// Arrange: spy to make objectMapper.readValue throw
		SQSRuleResponseListenerService spyListener = spy(new SQSRuleResponseListenerService(workflowService));
		ObjectMapper spyMapper = mock(ObjectMapper.class);
		doThrow(JsonProcessingException.class).when(spyMapper).readValue(anyString(),
				any(com.fasterxml.jackson.core.type.TypeReference.class));

		// replace private objectMapper via reflection
		java.lang.reflect.Field f = SQSRuleResponseListenerService.class.getDeclaredField("objectMapper");
		f.setAccessible(true);
		f.set(spyListener, spyMapper);

		// Act
		spyListener.handleRuleServiceSqsMessage("{\"foo\":\"bar\"}");

		// Assert: workflowService never called
		verifyNoInteractions(workflowService);
	}
}

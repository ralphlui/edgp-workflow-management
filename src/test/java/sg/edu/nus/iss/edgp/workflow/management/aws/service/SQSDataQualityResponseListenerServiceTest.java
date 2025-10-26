package sg.edu.nus.iss.edgp.workflow.management.aws.service;


import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

@ExtendWith(MockitoExtension.class)
class SQSDataQualityResponseListenerServiceTest {

    @Mock
    private WorkflowService workflowService;

    @InjectMocks
    private SQSDataQualityResponseListenerService listener;

    @Captor
    private ArgumentCaptor<Map<String, Object>> mapCaptor;

    private String validJson;
    private String invalidJson;
    private String arrayJson;
    private String stringJson;

    @BeforeEach
    void setUp() {
        validJson =
            "{\n" +
            "  \"workflowId\": \"wf-789\",\n" +
            "  \"status\": \"COMPLETED\",\n" +
            "  \"metrics\": { \"passed\": 120, \"failed\": 3, \"coverage\": 0.98 }\n" +
            "}";

        invalidJson = "{ invalid-json: ";
        arrayJson   = "[ {\"x\":1}, {\"y\":2} ]";
        stringJson  = "\"not-an-object\"";
    }

    @Test
    void handleRuleServiceSqsMessage_validJson_callsWorkflowServiceOnce() {
        // Act
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(validJson));

        // Assert
        verify(workflowService, times(1)).updateDataQualityWorkflowStatus(mapCaptor.capture());
        Map<String, Object> parsed = mapCaptor.getValue();

        // spot-check fields
        org.junit.jupiter.api.Assertions.assertEquals("wf-789", parsed.get("workflowId"));
        org.junit.jupiter.api.Assertions.assertEquals("COMPLETED", parsed.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) parsed.get("metrics");
        org.junit.jupiter.api.Assertions.assertNotNull(metrics);
        org.junit.jupiter.api.Assertions.assertTrue(metrics.containsKey("passed"));
        org.junit.jupiter.api.Assertions.assertTrue(metrics.containsKey("failed"));
        org.junit.jupiter.api.Assertions.assertTrue(metrics.containsKey("coverage"));
    }

    @Test
    void handleRuleServiceSqsMessage_invalidJson_doesNotCallService() {
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(invalidJson));
        verify(workflowService, never()).updateDataQualityWorkflowStatus(anyMap());
    }

    @Test
    void handleRuleServiceSqsMessage_nullMessage_doesNotCallService() {
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(null));
        verify(workflowService, never()).updateDataQualityWorkflowStatus(anyMap());
    }

    @Test
    void handleRuleServiceSqsMessage_nonObjectJson_array_doesNotCallService() {
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(arrayJson));
        verify(workflowService, never()).updateDataQualityWorkflowStatus(anyMap());
    }

    @Test
    void handleRuleServiceSqsMessage_nonObjectJson_string_doesNotCallService() {
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(stringJson));
        verify(workflowService, never()).updateDataQualityWorkflowStatus(anyMap());
    }

    @Test
    void handleRuleServiceSqsMessage_serviceThrows_isCaughtAndNotPropagated() {
        doThrow(new RuntimeException("boom"))
            .when(workflowService)
            .updateDataQualityWorkflowStatus(anyMap());

        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(validJson));
        verify(workflowService, times(1)).updateDataQualityWorkflowStatus(anyMap());
    }
}

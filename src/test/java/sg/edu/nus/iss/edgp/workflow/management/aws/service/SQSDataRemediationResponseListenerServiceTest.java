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

import sg.edu.nus.iss.edgp.workflow.management.service.impl.DataRemediationService;

@ExtendWith(MockitoExtension.class)
class SQSDataRemediationResponseListenerServiceTest {

    @Mock
    private DataRemediationService dataRemediationService;

    // We don't need to mock ObjectMapper because the class constructs its own and we can pass real JSON strings

    @InjectMocks
    private SQSDataRemediationResponseListenerService listener;

    @Captor
    private ArgumentCaptor<Map<String, Object>> mapCaptor;

    private String validJson;

    @BeforeEach
    void setUp() {
        // Example valid JSON that should parse to a Map<String, Object>
        validJson = """
            {
              "workflowId": "wf-123",
              "status": "SUCCESS",
              "details": {
                "remediatedRecords": 5,
                "skippedRecords": 1
              }
            }
            """;
    }

    @Test
    void handleRuleServiceSqsMessage_validJson_callsServiceOnce() {
        // Act
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(validJson));

        // Assert
        verify(dataRemediationService, times(1)).updateDataRemediationResponse(mapCaptor.capture());
        Map<String, Object> parsed = mapCaptor.getValue();

        // Spot-check a couple of fields
        // (Exact types may vary due to Jackson mapping of numbers -> Integer/Long; we just ensure keys exist)
        org.junit.jupiter.api.Assertions.assertEquals("wf-123", parsed.get("workflowId"));
        org.junit.jupiter.api.Assertions.assertEquals("SUCCESS", parsed.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> details = (Map<String, Object>) parsed.get("details");
        org.junit.jupiter.api.Assertions.assertNotNull(details);
        org.junit.jupiter.api.Assertions.assertTrue(details.containsKey("remediatedRecords"));
        org.junit.jupiter.api.Assertions.assertTrue(details.containsKey("skippedRecords"));
    }

    @Test
    void handleRuleServiceSqsMessage_invalidJson_doesNotCallService() {
        String invalidJson = "{ not-json: ";

        // Act
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(invalidJson));

        // Assert
        verify(dataRemediationService, never()).updateDataRemediationResponse(anyMap());
    }

    @Test
    void handleRuleServiceSqsMessage_serviceThrows_isCaughtAndNotPropagated() {
        doThrow(new RuntimeException("boom"))
            .when(dataRemediationService)
            .updateDataRemediationResponse(anyMap());

        // Act + Assert: method should swallow exception per current implementation
        assertDoesNotThrow(() -> listener.handleRuleServiceSqsMessage(validJson));

        // Still was invoked once
        verify(dataRemediationService, times(1)).updateDataRemediationResponse(anyMap());
    }
}

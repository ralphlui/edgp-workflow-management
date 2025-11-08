package sg.edu.nus.iss.edgp.workflow.management.aws.service;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SQSRawDataListenerServiceTest {

    private SQSRuleRequestListenerService requestListenerService;
    private ObjectMapper objectMapper;
    private SQSRawDataListenerService service;

    @BeforeEach
    void setUp() {
        requestListenerService = mock(SQSRuleRequestListenerService.class);
        objectMapper = mock(ObjectMapper.class);

        service = new SQSRawDataListenerService(requestListenerService, objectMapper);
        ReflectionTestUtils.setField(service, "requestListenerService", requestListenerService);
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
    }

    @Test
    void handleRawDataMessage_forwardsOnValidJson() throws Exception {
        
        String json = "{\"data_entry\":{\"id\":\"123\",\"name\":\"Alice\"},\"meta\":\"ok\"}";
        Map<String, Object> parsed = Map.of(
                "data_entry", Map.of("id", "123", "name", "Alice"),
                "meta", "ok"
        );

        when(objectMapper.readValue(eq(json), any(TypeReference.class))).thenReturn(parsed);

        service.handleRawDataMessage(json);

        ArgumentCaptor<Map<String, Object>> cap = ArgumentCaptor.forClass(Map.class);
        verify(requestListenerService, times(1)).forwardToRulesRequestQueue(cap.capture());
        assertEquals(parsed, cap.getValue());

        verify(objectMapper, times(1)).readValue(eq(json), any(TypeReference.class));
    }

    @Test
    void handleRawDataMessage_doesNotForwardOnInvalidJson() throws Exception {
       
        String badJson = "{ invalid-json }";
        when(objectMapper.readValue(eq(badJson), any(TypeReference.class)))
                .thenThrow(new JsonProcessingException("boom") {});

        service.handleRawDataMessage(badJson);

        verify(objectMapper, times(1)).readValue(eq(badJson), any(TypeReference.class));
        verifyNoInteractions(requestListenerService);
    }
}

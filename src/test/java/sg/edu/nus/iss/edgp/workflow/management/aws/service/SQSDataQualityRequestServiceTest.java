package sg.edu.nus.iss.edgp.workflow.management.aws.service;


import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@ExtendWith(MockitoExtension.class)
class SQSDataQualityRequestServiceTest {

    @Mock
    private SqsAsyncClient sqsAsyncClient;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private SQSDataQualityRequestService service;

    private static final String QUEUE_URL = "https://sqs.test.amazonaws.com/123/edgp-dataquality-req";

    @BeforeEach
    void injectQueueUrl() throws Exception {
        
        Field f = SQSDataQualityRequestService.class.getDeclaredField("dataQualityRequestQueueUrl");
        f.setAccessible(true);
        f.set(service, QUEUE_URL);
    }

    @Test
    void forwardToDataQualityRequestQueue_happyPath_sendsMessage() throws Exception {
        
        Map<String, Object> payload = Map.of("workflowId", "wf-123", "status", "READY");
        when(objectMapper.writeValueAsString(payload)).thenReturn("{\"workflowId\":\"wf-123\",\"status\":\"READY\"}");

        
        CompletableFuture<SendMessageResponse> future =
                CompletableFuture.completedFuture(SendMessageResponse.builder().messageId("mid-1").build());
        ArgumentCaptor<SendMessageRequest> reqCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        when(sqsAsyncClient.sendMessage(reqCaptor.capture())).thenReturn(future);
 
        assertDoesNotThrow(() -> service.forwardToDataQualityRequestQueue(payload));
 
        verify(sqsAsyncClient, times(1)).sendMessage(any(SendMessageRequest.class));
        SendMessageRequest sent = reqCaptor.getValue();
        org.junit.jupiter.api.Assertions.assertEquals(QUEUE_URL, sent.queueUrl());
        org.junit.jupiter.api.Assertions.assertEquals("{\"workflowId\":\"wf-123\",\"status\":\"READY\"}", sent.messageBody());

        
        future.join();
    }

    @Test
    void forwardToDataQualityRequestQueue_serializationFails_doesNotSend() throws Exception {
        
        Map<String, Object> payload = Map.of("bad", new Object());
        when(objectMapper.writeValueAsString(payload)).thenThrow(new JsonProcessingException("boom") {});

       
        assertDoesNotThrow(() -> service.forwardToDataQualityRequestQueue(payload));

        
        verify(sqsAsyncClient, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void forwardToDataQualityRequestQueue_asyncFailure_exceptionHandled() throws Exception {
        
        Map<String, Object> payload = Map.of("workflowId", "wf-err");
        when(objectMapper.writeValueAsString(payload)).thenReturn("{\"workflowId\":\"wf-err\"}");

        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(future);
 
        assertDoesNotThrow(() -> service.forwardToDataQualityRequestQueue(payload));
 
        future.completeExceptionally(new RuntimeException("SQS down"));

        
        assertDoesNotThrow(() -> future.handle((r, ex) -> null).get(100, TimeUnit.MILLISECONDS));

         
        verify(sqsAsyncClient, times(1)).sendMessage(any(SendMessageRequest.class));
    }
}

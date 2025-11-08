package sg.edu.nus.iss.edgp.workflow.management.connector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import sg.edu.nus.iss.edgp.workflow.management.api.connector.AdminAPICall;

class AdminAPICallTest {

    private AdminAPICall api;

    @BeforeEach
    void setUp() throws Exception {
        api = new AdminAPICall();
        // Inject adminURL via reflection since it's @Value-injected in production
        Field f = AdminAPICall.class.getDeclaredField("adminURL");
        f.setAccessible(true);
        // Trailing slash is fine; the code appends paths
        f.set(api, "http://fake-auth-url.com/");
    }

    @Test
    void validateActiveUser_success_returnsBody_andSendsCorrectHeadersAndUrl() throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        HttpClient.Builder mockBuilder = mock(HttpClient.Builder.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResp = (HttpResponse<String>) mock(HttpResponse.class);

        // Static mocking: HttpClient.newBuilder()
        try (MockedStatic<HttpClient> mocked = mockStatic(HttpClient.class)) {
            // Chain builder → connectTimeout → build → client
            mocked.when(HttpClient::newBuilder).thenReturn(mockBuilder);
            when(mockBuilder.connectTimeout(any(Duration.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);

            String expectedBody = "{\"status\":\"active\"}";
            when(mockResp.body()).thenReturn(expectedBody);
            // capture the request to assert URL and headers
            ArgumentCaptor<HttpRequest> reqCap = ArgumentCaptor.forClass(HttpRequest.class);
            when(mockClient.send(reqCap.capture(), any(HttpResponse.BodyHandler.class))).thenReturn(mockResp);

            // Call
            String result = api.validateActiveUser("user123", "Bearer abc123");

            // Verify body
            assertEquals(expectedBody, result);

            // Assert request built as expected
            HttpRequest sentReq = reqCap.getValue();
            assertNotNull(sentReq);

            // URL (double-slash is still valid if present)
            URI uri = sentReq.uri();
            assertTrue(uri.toString().endsWith("/users/profile"));

            // Headers
            assertEquals("Bearer abc123", sentReq.headers().firstValue("Authorization").orElse(null));
            assertEquals("user123", sentReq.headers().firstValue("X-User-Id").orElse(null));
            assertEquals("application/json", sentReq.headers().firstValue("Content-Type").orElse(null));
            assertEquals("GET", sentReq.method());
        }
    }

    @Test
    void validateActiveUser_exception_returnsEmptyString() throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        HttpClient.Builder mockBuilder = mock(HttpClient.Builder.class);

        try (MockedStatic<HttpClient> mocked = mockStatic(HttpClient.class)) {
            mocked.when(HttpClient::newBuilder).thenReturn(mockBuilder);
            when(mockBuilder.connectTimeout(any(Duration.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);

            when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                    .thenThrow(new RuntimeException("boom"));

            String out = api.validateActiveUser("userX", "Bearer y");
            assertEquals("", out);
        }
    }

    @Test
    void getAccessToken_success_returnsBody_andSendsCorrectHeadersAndUrl() throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        HttpClient.Builder mockBuilder = mock(HttpClient.Builder.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResp = (HttpResponse<String>) mock(HttpResponse.class);

        try (MockedStatic<HttpClient> mocked = mockStatic(HttpClient.class)) {
            mocked.when(HttpClient::newBuilder).thenReturn(mockBuilder);
            when(mockBuilder.connectTimeout(any(Duration.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);

            String expectedBody = "{\"accessToken\":\"token-123\"}";
            when(mockResp.body()).thenReturn(expectedBody);
            ArgumentCaptor<HttpRequest> reqCap = ArgumentCaptor.forClass(HttpRequest.class);
            when(mockClient.send(reqCap.capture(), any(HttpResponse.BodyHandler.class))).thenReturn(mockResp);

            String out = api.getAccessToken("alice@example.com");
            assertEquals(expectedBody, out);

            HttpRequest sent = reqCap.getValue();
            assertNotNull(sent);
            assertTrue(sent.uri().toString().endsWith("/users/accessToken"));
            assertEquals("alice@example.com", sent.headers().firstValue("X-User-Email").orElse(null));
            assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(null));
            assertEquals("GET", sent.method());
        }
    }

    @Test
    void getAccessToken_exception_returnsEmptyString() throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        HttpClient.Builder mockBuilder = mock(HttpClient.Builder.class);

        try (MockedStatic<HttpClient> mocked = mockStatic(HttpClient.class)) {
            mocked.when(HttpClient::newBuilder).thenReturn(mockBuilder);
            when(mockBuilder.connectTimeout(any(Duration.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);

            when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                    .thenThrow(new RuntimeException("timeout"));

            String out = api.getAccessToken("bob@example.com");
            assertEquals("", out);
        }
    }
}

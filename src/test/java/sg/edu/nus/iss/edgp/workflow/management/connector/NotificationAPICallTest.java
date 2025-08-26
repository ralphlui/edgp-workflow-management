package sg.edu.nus.iss.edgp.workflow.management.connector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.workflow.management.api.connector.NotificationAPICall;

import org.json.simple.JSONObject;

class NotificationAPICallTest {

	private MockedStatic<HttpClientBuilder> httpClientBuilderStaticMock;

	@AfterEach
	void tearDown() {
		if (httpClientBuilderStaticMock != null) {
			httpClientBuilderStaticMock.close();
		}
	}

	@Test
	void sendEmailWithAttachment_success_returnsParsedJson_andBuildsCorrectRequest() throws Exception {
		// Arrange
		NotificationAPICall api = new NotificationAPICall();
		ReflectionTestUtils.setField(api, "notificationURL", "https://api.example.com");

		// Mocks for HTTP client chain
		CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
		HttpClientBuilder mockBuilder = mock(HttpClientBuilder.class);
		CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

		// Prepare JSON response body
		String json = "{\"status\":\"OK\",\"message\":\"sent\"}";
		StringEntity responseEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
		when(mockResponse.getEntity()).thenReturn(responseEntity);

		// When execute is called, return our mock response
		when(mockClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

		// Static mocking for HttpClientBuilder.create()
		httpClientBuilderStaticMock = Mockito.mockStatic(HttpClientBuilder.class);
		httpClientBuilderStaticMock.when(HttpClientBuilder::create).thenReturn(mockBuilder);
		when(mockBuilder.setDefaultRequestConfig(any(RequestConfig.class))).thenReturn(mockBuilder);
		when(mockBuilder.build()).thenReturn(mockClient);

		// Temp file as attachment
		File temp = File.createTempFile("test-attachment", ".txt");
		temp.deleteOnExit();

		String token = "token123";

		// Act
		JSONObject result = api.sendEmailWithAttachment("user@example.com", "Subject here", "Body here", temp, token);

		// Assert parsed JSON
		assertEquals("OK", result.get("status"));
		assertEquals("sent", result.get("message"));

		// Capture the request to assert URL, headers, and multipart content type
		ArgumentCaptor<HttpPost> postCaptor = ArgumentCaptor.forClass(HttpPost.class);
		verify(mockClient, times(1)).execute(postCaptor.capture());

		HttpPost captured = postCaptor.getValue();
		assertEquals("https://api.example.com/send-email-with-attachment", captured.getURI().toString());

		// Authorization header check
		assertEquals("Bearer " + token, captured.getFirstHeader("Authorization").getValue());

		// Multipart content type check
		HttpEntity sentEntity = captured.getEntity();
		assertNotNull(sentEntity, "Request should have an entity");
		String contentType = sentEntity.getContentType().getValue();
		assertTrue(contentType.toLowerCase().contains("multipart/form-data"),
				() -> "Expected multipart/form-data, got: " + contentType);

		// (Optional) sanity: entity can be streamed
		byte[] preview = sentEntity.getContent().readNBytes(256);
		assertTrue(preview.length > 0, "Multipart body should not be empty");
	}

	@Test
	void sendEmailWithAttachment_badJson_returnsEmptyJsonObject() throws Exception {
		// Arrange
		NotificationAPICall api = new NotificationAPICall();
		ReflectionTestUtils.setField(api, "notificationURL", "https://api.example.com");

		CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
		HttpClientBuilder mockBuilder = mock(HttpClientBuilder.class);
		CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

		// Return non-JSON payload to trigger parse error path
		StringEntity responseEntity = new StringEntity("not-json",
				ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8));
		when(mockResponse.getEntity()).thenReturn(responseEntity);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

		httpClientBuilderStaticMock = Mockito.mockStatic(HttpClientBuilder.class);
		httpClientBuilderStaticMock.when(HttpClientBuilder::create).thenReturn(mockBuilder);
		when(mockBuilder.setDefaultRequestConfig(any(RequestConfig.class))).thenReturn(mockBuilder);
		when(mockBuilder.build()).thenReturn(mockClient);

		File temp = File.createTempFile("test-attachment", ".txt");
		temp.deleteOnExit();

		// Act
		JSONObject result = api.sendEmailWithAttachment("user@example.com", "S", "B", temp, "tok");

		// Assert: empty JSON object
		assertTrue(result.isEmpty(), "Expected empty JSON when parse fails");
	}
}
package sg.edu.nus.iss.edgp.workflow.management.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.workflow.management.api.connector.OrganizationAPICall;

class OrganizationAPICallTest {

	private OrganizationAPICall api;
	private HttpClient mockClient;
	private HttpClient.Builder mockBuilder;
	private MockedStatic<HttpClient> httpClientStaticMock;

	@BeforeEach
	void setUp() {
		api = new OrganizationAPICall();
		// Set the private @Value field
		ReflectionTestUtils.setField(api, "orgURL", "https://api.example.com");

		// Mock HttpClient.newBuilder() static call + builder chain
		mockClient = mock(HttpClient.class);
		mockBuilder = mock(HttpClient.Builder.class);

		httpClientStaticMock = mockStatic(HttpClient.class);
		httpClientStaticMock.when(HttpClient::newBuilder).thenReturn(mockBuilder);

		when(mockBuilder.connectTimeout(Duration.ofSeconds(30))).thenReturn(mockBuilder);
		when(mockBuilder.build()).thenReturn(mockClient);
	}

	@AfterEach
	void tearDown() {
		if (httpClientStaticMock != null) {
			httpClientStaticMock.close();
		}
	}

	@Test
	void validateActiveOrganization_success() throws Exception {
		// Arrange
		String orgId = "org-123";
		String authHeader = "Bearer abc123";

		@SuppressWarnings("unchecked")
		HttpResponse<String> mockResponse = (HttpResponse<String>) mock(HttpResponse.class);
		when(mockResponse.body()).thenReturn("{\"status\":\"ok\"}");

		// Stub the send() call
		when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResponse);

		// Act
		String result = api.validateActiveOrganization(orgId, authHeader);

		// Assert
		assertEquals("{\"status\":\"ok\"}", result);

		// Capture and inspect the built HttpRequest
		@SuppressWarnings("unchecked")
		ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
		verify(mockClient, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));

		HttpRequest sent = reqCaptor.getValue();
		assertEquals("GET", sent.method());
		assertEquals(URI.create("https://api.example.com/my-organization"), sent.uri());
		assertEquals(authHeader, sent.headers().firstValue("Authorization").orElse(null));
		assertEquals(orgId, sent.headers().firstValue("X-Org-Id").orElse(null));
		assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(null));

		// Verify builder chain was used as expected
		httpClientStaticMock.verify(HttpClient::newBuilder, times(1));
		verify(mockBuilder, times(1)).connectTimeout(Duration.ofSeconds(30));
		verify(mockBuilder, times(1)).build();
	}

	@Test
	void validateActiveOrganization_whenSendThrows_returnsEmptyString() throws Exception {
		String orgId = "org-err";
		String authHeader = "Bearer oops";

		when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
				.thenThrow(new IOException("network down"));

		String result = api.validateActiveOrganization(orgId, authHeader);

		assertEquals("", result);
		verify(mockClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
	}
}
package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.util.HashMap;

import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.workflow.management.api.connector.NotificationAPICall;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DataIngestionNotifierService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

@ExtendWith(MockitoExtension.class)
class DataIngestionNotifierServiceTest {

	@Mock
	private DynamicDynamoService dynamoService;
	@Mock
	private NotificationAPICall notificationAPICall;
	@Mock
	private JSONReader jsonReader;

	@InjectMocks
	private DataIngestionNotifierService service;

	@BeforeEach
	void setUp() {
		// Inject @Value fields
		ReflectionTestUtils.setField(service, "masterDataTaskTrackerTableName", "TaskTrackerTable");
		ReflectionTestUtils.setField(service, "masterDataHeaderTableName", "HeaderTable");
	}

	@Test
	void sendDataIngestionResult_success_sendsEmail() {
		// Arrange
		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-123");

		File csvMock = mock(File.class);
		when(csvMock.getAbsoluteFile()).thenReturn(csvMock);
		when(csvMock.getName()).thenReturn("customer-123.csv"); // should become "customer.csv"

		when(dynamoService.exportToCsv("TaskTrackerTable", fileInfo)).thenReturn(csvMock);
		when(dynamoService.getUploadUserByFileId("HeaderTable", "file-123")).thenReturn("alice@example.com");
		when(jsonReader.getAccessToken("alice@example.com")).thenReturn("Bearer token");

		JSONObject ok = new JSONObject();
		ok.put("success", true);
		when(notificationAPICall.sendEmailWithAttachment(eq("alice@example.com"), eq("Data Ingestion Result"),
				anyString(), eq(csvMock), eq("Bearer token"))).thenReturn(ok);

		// Act
		assertDoesNotThrow(() -> service.sendDataIngestionResult(fileInfo));

		// Assert
		verify(notificationAPICall, times(1)).sendEmailWithAttachment(eq("alice@example.com"),
				eq("Data Ingestion Result"),
				argThat(body -> body.contains("customer.csv") && body.contains("(This is an auto-generated email")),
				eq(csvMock), eq("Bearer token"));
	}

	@Test
	void sendDataIngestionResult_emailFailure_throwsRuntimeException() {
		// Arrange
		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-123");

		File csvMock = mock(File.class);
		when(csvMock.getAbsoluteFile()).thenReturn(csvMock);
		when(csvMock.getName()).thenReturn("data-999.csv");

		when(dynamoService.exportToCsv("TaskTrackerTable", fileInfo)).thenReturn(csvMock);
		when(dynamoService.getUploadUserByFileId("HeaderTable", "file-123")).thenReturn("bob@example.com");
		when(jsonReader.getAccessToken("bob@example.com")).thenReturn("Bearer token");

		JSONObject fail = new JSONObject();
		fail.put("success", false);
		fail.put("message", "SMTP error");
		when(notificationAPICall.sendEmailWithAttachment(anyString(), anyString(), anyString(), any(File.class),
				anyString())).thenReturn(fail);

		// Act + Assert
		RuntimeException ex = assertThrows(RuntimeException.class, () -> service.sendDataIngestionResult(fileInfo));
		assertTrue(ex.getMessage().contains("Failed to export workflow CSV and send email"));
		assertTrue(ex.getCause().getMessage().contains("Email sending failed"));
	}

	@Test
	void sendDataIngestionResult_noCsv_noFurtherActions() {
		// Arrange
		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-123");

		when(dynamoService.exportToCsv("TaskTrackerTable", fileInfo)).thenReturn(null);

		// Act
		assertDoesNotThrow(() -> service.sendDataIngestionResult(fileInfo));

		// Assert: nothing else should be called
		verify(dynamoService, never()).getUploadUserByFileId(anyString(), anyString());
		verify(jsonReader, never()).getAccessToken(anyString());
		verify(notificationAPICall, never()).sendEmailWithAttachment(anyString(), anyString(), anyString(), any(),
				anyString());
	}

}
package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicSQLService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;
// If you use AWS SDK v1, switch import to com.amazonaws.services.dynamodbv2.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ExtendWith(MockitoExtension.class)
class WorkflowServiceTest {

	@Mock
	private DynamicDynamoService dynamoService;

	@Mock
	private DynamicSQLService dynamicSQLService;

	// Spy the SUT so we can stub private helpers like dynamoItemToJavaMap via
	// spy/doReturn
	@Spy
	@InjectMocks
	private WorkflowService service;

	private static final String TABLE = "master_task_tracker";

	@BeforeEach
	void setUp() throws Exception {
		// Set the table name if it's a private field
		Field f = WorkflowService.class.getDeclaredField("masterDataTaskTrackerTableName");
		f.setAccessible(true);
		f.set(service, TABLE);
	}

	@Test
	void updateWorkflowStatus_success_insertsCleanData() {
		// Arrange
		Map<String, Object> data = Map.of("id", "wf-123");
		List<Map<String, Object>> failedValidations = List.of(Map.of("code", "X1", "msg", "oops"));

		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS"); // case-insensitive match in insetCleanMasterData
		raw.put("data", data);
		raw.put("failed_validations", failedValidations);
		raw.put("domain_name", "my_domain_tbl");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);

		Map<String, AttributeValue> stored = new HashMap<>();
		stored.put("id", AttributeValue.builder().s("wf-123").build());
		stored.put("created_date", AttributeValue.builder().s("2025-08-01T12:00:00Z").build());
		stored.put("final_status", AttributeValue.builder().s("PENDING").build());
		stored.put("rule_status", AttributeValue.builder().s("PENDING").build());
		stored.put("business_key", AttributeValue.builder().s("BK-9").build());
		stored.put("amount", AttributeValue.builder().n("42").build());
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123")).thenReturn(stored);

		// Stub the conversion helper to return a mutable map we can assert on later
		Map<String, Object> converted = new LinkedHashMap<>();
		converted.put("id", "wf-123");
		converted.put("created_date", "2025-08-01T12:00:00Z");
		converted.put("final_status", "PENDING");
		converted.put("rule_status", "PENDING");
		converted.put("business_key", "BK-9");
		converted.put("amount", 42);
		doReturn(converted).when(service).dynamoItemToJavaMap(stored);

		ArgumentCaptor<WorkflowStatus> wsCaptor = ArgumentCaptor.forClass(WorkflowStatus.class);
		ArgumentCaptor<Map<String, Object>> cleanMapCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<String> domainTableCaptor = ArgumentCaptor.forClass(String.class);

		// Act
		service.updateWorkflowStatus(raw);

		// Assert: workflow status persisted with expected fields
		verify(dynamoService).updateWorkflowStatus(eq(TABLE), wsCaptor.capture());
		WorkflowStatus saved = wsCaptor.getValue();
		assertEquals("wf-123", saved.getId());
		assertEquals("SUCCESS", saved.getFinalStatus());
		assertEquals("SUCCESS", saved.getRuleStatus());
		assertEquals(List.copyOf(failedValidations), saved.getFailedValidations());

		// Assert: dynamicSQLService called with cleaned map (keys removed)
		verify(dynamicSQLService).buildCreateTableSQL(cleanMapCaptor.capture(), domainTableCaptor.capture());
		Map<String, Object> cleaned = cleanMapCaptor.getValue();
		assertFalse(cleaned.containsKey("id"), "id should be removed");
		assertFalse(cleaned.containsKey("created_date"), "created_date should be removed");
		assertFalse(cleaned.containsKey("final_status"), "final_status should be removed");
		assertFalse(cleaned.containsKey("rule_status"), "rule_status should be removed");
		// business fields remain
		assertEquals("BK-9", cleaned.get("business_key"));
		assertEquals(42, cleaned.get("amount"));
		assertEquals("my_domain_tbl", domainTableCaptor.getValue());
	}

	@Test
	@DisplayName("Creates table when it doesn't exist")
	void updateWorkflowStatus_createsTableIfMissing() {
		// Arrange
		Map<String, Object> data = Map.of("id", "wf-123");
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", data);
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "my_domain_tbl");

		when(dynamoService.tableExists(TABLE)).thenReturn(false);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));

		// conversion stub to keep insetCleanMasterData happy
		doReturn(new HashMap<>(Map.of("business_key", "BK-1"))).when(service).dynamoItemToJavaMap(anyMap());

		// Act
		service.updateWorkflowStatus(raw);

		// Assert
		verify(dynamoService).createTable(TABLE);
	}

	@Test
	void updateWorkflowStatus_missingExistingData_throws() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", Map.of("id", "wf-404"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "domain");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-404")).thenReturn(Collections.emptyMap());

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.updateWorkflowStatus(raw));
		assertTrue(ex.getMessage().contains("error"));
	}

	@Test
	void updateWorkflowStatus_noInsertWhenNotSuccessOrNoDomain() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "FAILED"); // not success
		raw.put("data", Map.of("id", "wf-123"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", ""); // empty

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));

		// Act
		service.updateWorkflowStatus(raw);

		// Assert
		verify(dynamicSQLService, never()).buildCreateTableSQL(anyMap(), anyString());
	}

	@Test
	void updateWorkflowStatus_wrapsExceptions() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", Map.of("id", "wf-123"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "domain");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));
		doThrow(new RuntimeException("boom")).when(dynamoService).updateWorkflowStatus(eq(TABLE),
				any(WorkflowStatus.class));

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.updateWorkflowStatus(raw));
		assertTrue(ex.getMessage().contains("An error occurred while updating workflow status"));
	}
}

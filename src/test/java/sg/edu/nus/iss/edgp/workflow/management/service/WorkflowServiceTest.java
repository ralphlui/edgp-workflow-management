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

import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
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

	@Spy
	@InjectMocks
	private WorkflowService service;

	private static final String TABLE = "master_task_tracker";
	private static final String TRACKER_TABLE = "master_task_tracker";
	private static final String HEADER_TABLE = "master_header";

	@BeforeEach
	void setUp() throws Exception {
		// Set the table name if it's a private field
		Field f = WorkflowService.class.getDeclaredField("masterDataTaskTrackerTableName");
		f.setAccessible(true);
		f.set(service, TABLE);
		setPrivateField(service, "masterDataTaskTrackerTableName", TRACKER_TABLE);
		setPrivateField(service, "masterDataHeaderTableName", HEADER_TABLE);
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

	private static void setPrivateField(Object target, String field, Object value) throws Exception {
		Field f = target.getClass().getDeclaredField(field);
		f.setAccessible(true);
		f.set(target, value);
	}

	@Test
	void retrieveDataList_happyPath() {
		// Arrange
		String fileId = "file-123";
		String orgId = "org-9";
		SearchRequest searchRequest = new SearchRequest();

		Map<String, AttributeValue> item1 = new HashMap<>();
		item1.put("id", AttributeValue.builder().s("r1").build());
		item1.put("final_status", AttributeValue.builder().s("SUCCESS").build());

		Map<String, AttributeValue> item2 = new HashMap<>();
		item2.put("id", AttributeValue.builder().s("r2").build());
		item2.put("final_status", AttributeValue.builder().s("FAIL").build());

		Map<String, Object> retrieveResult = new HashMap<>();
		retrieveResult.put("items", List.of(item1, item2));
		retrieveResult.put("totalCount", 2);
		when(dynamoService.retrieveDataList(TRACKER_TABLE, fileId, searchRequest, orgId)).thenReturn(retrieveResult);

		Map<String, AttributeValue> headerRecord = new HashMap<>();
		headerRecord.put("file_name", AttributeValue.builder().s("invoices_aug.csv").build());
		when(dynamoService.getFileDataByFileId(HEADER_TABLE, fileId)).thenReturn(headerRecord);

		// Spy conversion from Dynamo item to Java map
		Map<String, Object> converted1 = new LinkedHashMap<>(
				Map.of("id", "r1", "final_status", "SUCCESS", "amount", 100));
		Map<String, Object> converted2 = new LinkedHashMap<>(Map.of("id", "r2", "final_status", "FAIL", "amount", 200));
		doReturn(converted1).when(service).dynamoItemToJavaMap(item1);
		doReturn(converted2).when(service).dynamoItemToJavaMap(item2);

		// Act
		List<Map<String, Object>> out = service.retrieveDataList(fileId, searchRequest, orgId);

		// Assert: 3 elements (2 items + 1 summary at end)
		assertEquals(3, out.size());

		Map<String, Object> out1 = out.get(0);
		Map<String, Object> out2 = out.get(1);
		Map<String, Object> summary = out.get(2);

		// fileName added to each item
		assertEquals("invoices_aug.csv", out1.get("fileName"));
		assertEquals("invoices_aug.csv", out2.get("fileName"));

		// Original fields preserved
		assertEquals("r1", out1.get("id"));
		assertEquals("SUCCESS", out1.get("final_status"));
		assertEquals(100, out1.get("amount"));

		assertEquals("r2", out2.get("id"));
		assertEquals("FAIL", out2.get("final_status"));
		assertEquals(200, out2.get("amount"));

		// Summary map contains counts and totalCount; is last element
		assertEquals(2, summary.get("totalCount"));
		assertEquals(1, summary.get("successRecords"));
		assertEquals(1, summary.get("failedRecords"));
	}

	@Test
	void retrieveDataList_blankFileId_skipsHeaderLookup() {
		// Arrange
		String fileId = "   "; // blank after trim check in caller
		SearchRequest searchRequest = new SearchRequest();

		Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("r1").build(), "final_status",
				AttributeValue.builder().s("SUCCESS").build());
		Map<String, Object> retrieveResult = new HashMap<>();
		retrieveResult.put("items", List.of(item));
		retrieveResult.put("totalCount", 1);
		when(dynamoService.retrieveDataList(eq(TRACKER_TABLE), anyString(), any(), anyString()))
				.thenReturn(retrieveResult);

		doReturn(new LinkedHashMap<>(Map.of("id", "r1", "final_status", "SUCCESS"))).when(service)
				.dynamoItemToJavaMap(anyMap());

		// Act
		List<Map<String, Object>> out = service.retrieveDataList(fileId, searchRequest, "org");

		// Assert
		verify(dynamoService, never()).getFileDataByFileId(anyString(), anyString());
		assertNull(out.get(0).get("fileName"), "fileName should not be present when fileId is blank");
		Map<String, Object> summary = out.get(1);
		assertEquals(1, summary.get("totalCount"));
		assertEquals(1, summary.get("successRecords"));
		assertEquals(0, summary.get("failedRecords"));
	}

	@Test
	void retrieveDataList_wrapsExceptions() {
		// Arrange
		when(dynamoService.retrieveDataList(anyString(), anyString(), any(), anyString()))
				.thenThrow(new RuntimeException("boom"));

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.retrieveDataList("file", new SearchRequest(), "org"));
		assertTrue(ex.getMessage().contains("An error occurred while retireving data list"));
	}
}

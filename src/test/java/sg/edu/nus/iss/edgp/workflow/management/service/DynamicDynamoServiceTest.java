package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicDynamoServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@ExtendWith(MockitoExtension.class)
public class DynamicDynamoServiceTest {

	@Mock
	private DynamoDbClient dynamoDbClient;

	@InjectMocks
	private DynamicDynamoService service;

	@BeforeEach
	void setUp() {
		service = spy(new DynamicDynamoService(dynamoDbClient));
	}

	@Test
	void tableExists_returnsTrue_whenDescribeSucceeds() {
		when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
				.thenReturn(DescribeTableResponse.builder().build());

		boolean exists = service.tableExists("Users");

		assertTrue(exists);
		verify(dynamoDbClient).describeTable(any(DescribeTableRequest.class));
	}

	@Test
	void tableExists_returnsFalse_whenTableMissing() {
		when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
				.thenThrow(ResourceNotFoundException.builder().message("Not found").build());

		boolean exists = service.tableExists("MissingTable");

		assertFalse(exists);
		verify(dynamoDbClient).describeTable(any(DescribeTableRequest.class));
	}

	@Test
	void tableExists_propagatesOtherDynamoErrors() {
		when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
				.thenThrow(InternalServerErrorException.builder().message("Oops").build());

		assertThrows(DynamoDbException.class, () -> service.tableExists("AnyTable"));

		verify(dynamoDbClient).describeTable(any(DescribeTableRequest.class));
	}

	@Test
	void createTable_buildsExpectedRequest_callsSdk_andWaits() {

		String tableName = "MyTable";
		when(dynamoDbClient.createTable(any(CreateTableRequest.class)))
				.thenReturn(CreateTableResponse.builder().build());
		doNothing().when(service).waitForTableToBecomeActive(tableName);

		service.createTable(tableName);

		ArgumentCaptor<CreateTableRequest> reqCap = ArgumentCaptor.forClass(CreateTableRequest.class);
		verify(dynamoDbClient).createTable(reqCap.capture());

		CreateTableRequest req = reqCap.getValue();
		assertEquals(tableName, req.tableName());
		assertEquals(1, req.keySchema().size());
		KeySchemaElement kse = req.keySchema().get(0);
		assertEquals("id", kse.attributeName());

		assertEquals(1, req.attributeDefinitions().size());
		AttributeDefinition ad = req.attributeDefinitions().get(0);
		assertEquals("id", ad.attributeName());
		assertEquals(ScalarAttributeType.S, ad.attributeType());
		assertEquals(BillingMode.PAY_PER_REQUEST, req.billingMode());

		verify(service).waitForTableToBecomeActive(tableName);
	}

	@Test
	void createTable_propagatesError_andDoesNotWait() {
		String tableName = "Existing";
		when(dynamoDbClient.createTable(any(CreateTableRequest.class)))
				.thenThrow(ResourceInUseException.builder().message("already exists").build());

		assertThrows(DynamoDbException.class, () -> service.createTable(tableName));

		verify(service, never()).waitForTableToBecomeActive(anyString());
	}

	@Test
	void returnsFirstItem_andBuildsRequestCorrectly() {
		String table = "tbl";
		String id = "abc-123";

		Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s(id).build(), "status",
				AttributeValue.builder().s("READY").build());

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(List.of(item)).build());

		Map<String, AttributeValue> result = service.getDataByWorkflowStatusId(table, id);

		// returned first item
		assertNotNull(result);
		assertEquals(id, result.get("id").s());
		assertEquals("READY", result.get("status").s());

		// request correctness
		ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scan(cap.capture());
		ScanRequest sent = cap.getValue();
		assertEquals(table, sent.tableName());
		assertEquals("id = :id", sent.filterExpression());
		assertTrue(sent.expressionAttributeValues().containsKey(":id"));
		assertEquals(id, sent.expressionAttributeValues().get(":id").s());
	}

	@Test
	void wrapsClientError_inCustomException1() {
		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenThrow(DynamoDbException.builder().message("boom").build());

		DynamicDynamoServiceException ex = assertThrows(DynamicDynamoServiceException.class,
				() -> service.getDataByWorkflowStatusId("tbl", "123"));

		assertTrue(ex.getMessage().toLowerCase().contains("workflow status id"));
	}

	@Test
	void returnsNull_whenNoItems() {
		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.emptyList()).build());

		Map<String, AttributeValue> out = service.getFileDataByFileId("files", "f-123");

		assertNull(out);
		verify(dynamoDbClient).scan(any(ScanRequest.class));
	}

	@Test
	void returnsFirstItem_andBuildsRequestCorrectly1() {
		String table = "files";
		String id = "f-123";
		Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s(id).build(), "name",
				AttributeValue.builder().s("photo.jpg").build());

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(List.of(item)).build());

		Map<String, AttributeValue> result = service.getFileDataByFileId(table, id);

		// returned first item
		assertNotNull(result);
		assertEquals(id, result.get("id").s());
		assertEquals("photo.jpg", result.get("name").s());

		// verify the request details
		ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scan(cap.capture());
		ScanRequest sent = cap.getValue();
		assertEquals(table, sent.tableName());
		assertEquals("id = :id", sent.filterExpression());
		assertTrue(sent.expressionAttributeValues().containsKey(":id"));
		assertEquals(id, sent.expressionAttributeValues().get(":id").s());
	}

	@Test
	void wrapsClientError_inCustomException() {
		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenThrow(DynamoDbException.builder().message("boom").build());

		assertThrows(DynamicDynamoServiceException.class, () -> service.getFileDataByFileId("files", "f-123"));
	}

	private WorkflowStatus mockWs(String id, String ruleStatus, String finalStatus,
			List<Map<String, Object>> failedValidations) {
		WorkflowStatus ws = mock(WorkflowStatus.class);
		when(ws.getId()).thenReturn(id);
		when(ws.getRuleStatus()).thenReturn(ruleStatus);
		when(ws.getFinalStatus()).thenReturn(finalStatus);
		when(ws.getFailedValidations()).thenReturn(failedValidations);
		return ws;
	}

	@Test
	void doesNothing_whenNoFieldsToUpdate() {
		WorkflowStatus ws = mockWs("id-1", null, null, null);

		service.updateWorkflowStatus("tbl", ws);

		verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
	}

	@Test
	void updates_onlyRuleStatus() {
		WorkflowStatus ws = mockWs("id-2", "IN_PROGRESS", null, null);
		when(dynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

		service.updateWorkflowStatus("tbl", ws);

		ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
		verify(dynamoDbClient).updateItem(cap.capture());
		UpdateItemRequest sent = cap.getValue();

		assertEquals("tbl", sent.tableName());
		assertEquals(AttributeValue.builder().s("id-2").build(), sent.key().get("id"));
		assertEquals(ReturnValue.ALL_NEW, sent.returnValues());

// Expression and maps
		assertEquals("SET #rs = :rs", sent.updateExpression());
		assertEquals("rule_status", sent.expressionAttributeNames().get("#rs"));
		assertEquals("IN_PROGRESS", sent.expressionAttributeValues().get(":rs").s());
// should not contain others
		assertFalse(sent.expressionAttributeNames().containsKey("#fs"));
		assertFalse(sent.expressionAttributeNames().containsKey("#fv"));
	}

	@Test
	void updates_ruleAndFinal_inOrder_withComma() {
		WorkflowStatus ws = mockWs("id-3", "DONE", "SUCCESS", null);
		when(dynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

		service.updateWorkflowStatus("tbl", ws);

		ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
		verify(dynamoDbClient).updateItem(cap.capture());
		UpdateItemRequest sent = cap.getValue();

		String expr = sent.updateExpression();
		assertEquals("SET #rs = :rs, #fs = :fs", expr); // exact order from code

		assertEquals("rule_status", sent.expressionAttributeNames().get("#rs"));
		assertEquals("final_status", sent.expressionAttributeNames().get("#fs"));
		assertEquals("DONE", sent.expressionAttributeValues().get(":rs").s());
		assertEquals("SUCCESS", sent.expressionAttributeValues().get(":fs").s());
	}

	@Test
	void updates_failedValidations_withListAppend_andIfNotExists() {
		WorkflowStatus ws = mockWs("id-4", null, null, Collections.emptyList());
		when(dynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

		service.updateWorkflowStatus("tbl", ws);

		ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
		verify(dynamoDbClient).updateItem(cap.capture());
		UpdateItemRequest sent = cap.getValue();

		assertTrue(sent.updateExpression().startsWith("SET "));
// attribute names
		assertEquals("failed_validations", sent.expressionAttributeNames().get("#fv"));

// attribute values exist and are lists
		AttributeValue fv = sent.expressionAttributeValues().get(":fv");
		AttributeValue empty = sent.expressionAttributeValues().get(":empty");
		assertNotNull(fv);
		assertNotNull(empty);
		assertNotNull(fv.l()); // list present (may be empty)
		assertNotNull(empty.l()); // empty list placeholder
	}

	@Test
	void wrapsClientError_inException() {
		WorkflowStatus ws = mockWs("id-5", "ANY", null, null);
		when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
				.thenThrow(DynamoDbException.builder().message("boom").build());

		DynamicDynamoServiceException ex = assertThrows(DynamicDynamoServiceException.class,
				() -> service.updateWorkflowStatus("tbl", ws));

		assertTrue(ex.getMessage().toLowerCase().contains("error updating workflow status"));
	}

	// Helper to build a minimal item map with required keys
	private static Map<String, AttributeValue> item(String id, String orgId, String fileId, String finalStatus) {
		Map<String, AttributeValue> m = new HashMap<>();
		m.put("id", AttributeValue.builder().s(id).build());
		m.put("organization_id", AttributeValue.builder().s(orgId).build());
		if (fileId != null)
			m.put("file_id", AttributeValue.builder().s(fileId).build());
		if (finalStatus != null)
			m.put("final_status", AttributeValue.builder().s(finalStatus).build());
		return m;
	}

	@Test
	void retrieveDataList_WithPagination_ReturnsRequestedPageSlice() {
		String table = "my-table";
		String orgId = "org-123";

		// Build 15 items -> sorted by id "id-01".."id-15"
		List<Map<String, AttributeValue>> all = IntStream.rangeClosed(1, 15)
				.mapToObj(i -> item(String.format("id-%02d", i), orgId, "file-9", "done")).collect(Collectors.toList());

		when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(all)

				.lastEvaluatedKey(Collections.emptyMap()).build());

		SearchRequest search = mock(SearchRequest.class);
		when(search.getStatus()).thenReturn(null);
		when(search.getPage()).thenReturn(2);
		when(search.getSize()).thenReturn(5);

		Map<String, Object> result = service.retrieveDataList(table, "file-9", search, orgId);

		assertEquals(15, result.get("totalCount"));

		@SuppressWarnings("unchecked")
		List<Map<String, AttributeValue>> items = (List<Map<String, AttributeValue>>) result.get("items");
		List<String> ids = items.stream().map(m -> m.get("id").s()).collect(Collectors.toList());
		assertEquals(Arrays.asList("id-06", "id-07", "id-08", "id-09", "id-10"), ids);
	}

	@Test
	void retrieveDataList_PaginationBeyondRange_ReturnsEmptyList() {
		String table = "my-table";
		String orgId = "org-123";

		when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder()
				.items(Arrays.asList(item("a", orgId, null, "done"), item("b", orgId, null, "done")))
				.lastEvaluatedKey(Collections.emptyMap()).build());

		SearchRequest search = mock(SearchRequest.class);
		when(search.getStatus()).thenReturn(null);
		when(search.getPage()).thenReturn(5); // 5th page of size 10 is out of range
		when(search.getSize()).thenReturn(10);

		Map<String, Object> result = service.retrieveDataList(table, null, search, orgId);

		assertEquals(2, result.get("totalCount"));
		@SuppressWarnings("unchecked")
		List<Map<String, AttributeValue>> items = (List<Map<String, AttributeValue>>) result.get("items");
		assertTrue(items.isEmpty());
	}

	private static AttributeValue S(String s) {
		return AttributeValue.builder().s(s).build();
	}

	private static Map<String, AttributeValue> itemBase(String id, String fileId, String finalStatus) {
		Map<String, AttributeValue> m = new LinkedHashMap<>();
		m.put("id", S(id));
		m.put("file_id", S(fileId));
		m.put("organization_id", S("org-1"));
		m.put("domain_name", S("domain"));
		m.put("policy_id", S("policy"));
		m.put("uploaded_by", S("user"));
		m.put("created_date", S("2025-01-01T00:00:00Z"));
		m.put("final_status", S(finalStatus));
		// include a couple of non-excluded fields to be exported
		m.put("col1", S("v1"));
		m.put("col2", S("v2"));
		return m;
	}

	private static AttributeValue failedValidation(String rule, String col, String msg, String status) {
		Map<String, AttributeValue> m = new LinkedHashMap<>();
		m.put("rule_name", S(rule));
		m.put("column_name", S(col));
		m.put("error_message", S(msg));
		m.put("status", S(status));
		return AttributeValue.builder().m(m).build();
	}

	private static List<String> readAllLines(File f) throws IOException {
		return Files.readAllLines(f.toPath());
	}

	@Test
	void exportToCsv_success_topLevelFailedValidations() throws Exception {
		// Arrange: item with top-level "failed_validations"
		Map<String, AttributeValue> item = itemBase("1", "file-123", "done");
		item.put("failed_validations",
				AttributeValue.builder().l(Arrays.asList(failedValidation("ruleA", "colA", "msgA", "FAILED"),
						failedValidation("ruleB", "colB", "msgB", "FAILED"))).build());

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.singletonList(item)).build());

		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-123");
		fileInfo.put("name", "export.csv");

		// Act
		File csv = service.exportToCsv("tbl", fileInfo);

		// Assert basic scan request correctness
		ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scan(captor.capture());
		ScanRequest sent = captor.getValue();
		assertEquals("tbl", sent.tableName());
		assertTrue(sent.filterExpression().contains("file_id = :file_id"));
		assertEquals("file-123", sent.expressionAttributeValues().get(":file_id").s());

		// Assert file content
		List<String> lines = readAllLines(csv);
		assertEquals(2, lines.size(), "header + 1 data row expected");

		String header = lines.get(0);
		// Should not contain excluded default fields
		assertFalse(header.contains("file_id"));
		assertFalse(header.contains("organization_id"));
		assertFalse(header.contains("id"));
		// Should contain included fields and appended special columns
		assertTrue(header.contains("col1"));
		assertTrue(header.contains("col2"));
		assertTrue(header.contains("failed_rule_names"));
		assertTrue(header.contains("failed_column_names"));
		assertTrue(header.contains("failed_error_messages"));
		assertTrue(header.contains("failed_statuses"));
		assertTrue(header.contains("final_status"));

		String row = lines.get(1);
		// Values from our base fields
		assertTrue(row.contains("v1"));
		assertTrue(row.contains("v2"));
		// Aggregated semicolon-joined lists
		assertTrue(row.contains("ruleA;ruleB"));
		assertTrue(row.contains("colA;colB"));
		assertTrue(row.contains("msgA;msgB"));
		assertTrue(row.contains("FAILED;FAILED"));
		// Final status echoed
		assertTrue(row.contains("done"));

		// Cleanup
		assertTrue(csv.delete());
	}

	@Test
	void exportToCsv_success_nestedFailedValidationsUnderRuleStatus() throws Exception {
		// Arrange: item with nested rule_status.failed_validations (no top-level
		// failed_validations)
		Map<String, AttributeValue> nestedMap = new LinkedHashMap<>();
		nestedMap.put("failed_validations", AttributeValue.builder()
				.l(Collections.singletonList(failedValidation("ruleX", "colX", "msgX", "WARN"))).build());

		Map<String, AttributeValue> item = itemBase("2", "file-9", "in_progress");
		item.put("rule_status", AttributeValue.builder().m(nestedMap).build());

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.singletonList(item)).build());

		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-9");
		fileInfo.put("name", "rpt.csv");

		// Act
		File csv = service.exportToCsv("tbl2", fileInfo);

		// Assert CSV content includes our nested values
		List<String> lines = readAllLines(csv);
		assertEquals(2, lines.size());

		String header = lines.get(0);
		String row = lines.get(1);

		assertTrue(header.contains("failed_rule_names"));
		assertTrue(row.contains("ruleX"));
		assertTrue(row.contains("colX"));
		assertTrue(row.contains("msgX"));
		assertTrue(row.contains("WARN"));
		assertTrue(row.contains("in_progress"));

		assertTrue(csv.delete());
	}

	@Test
	void exportToCsv_wrapsDynamoErrors() {
		when(dynamoDbClient.scan(any(ScanRequest.class))).thenThrow(SdkException.create("boom", null));

		HashMap<String, String> fileInfo = new HashMap<>();
		fileInfo.put("id", "file-err");
		fileInfo.put("name", "err.csv");

		assertThrows(DynamicDynamoServiceException.class, () -> service.exportToCsv("tbl", fileInfo));
	}

	@Test
	void returnsUploadedBy_whenItemExistsWithUploadedBy() {
		String table = "tbl";
		String id = "file-123";

		Map<String, AttributeValue> item = new HashMap<>();
		item.put("id", S(id));
		item.put("uploaded_by", S("alice@example.com"));

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.singletonList(item)).build());

		String result = service.getUploadUserByFileId(table, id);
		assertEquals("alice@example.com", result);

		// verify request construction
		ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scan(captor.capture());
		ScanRequest sent = captor.getValue();
		assertEquals(table, sent.tableName());
		assertEquals("id = :id", sent.filterExpression());
		assertEquals(id, sent.expressionAttributeValues().get(":id").s());
	}

	@Test
	void returnsNull_whenItemExistsButUploadedByMissing() {
		String table = "tbl";
		String id = "file-456";

		Map<String, AttributeValue> item = new HashMap<>();
		item.put("id", S(id));
		// no uploaded_by

		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.singletonList(item)).build());

		String result = service.getUploadUserByFileId(table, id);
		assertNull(result);
	}

	@Test
	void returnsNull_whenNoItemsFound() {
		when(dynamoDbClient.scan(any(ScanRequest.class)))
				.thenReturn(ScanResponse.builder().items(Collections.emptyList()).build());

		String result = service.getUploadUserByFileId("tbl", "file-789");
		assertNull(result);
	}

	@Test
	void throws_whenIdNullOrBlank() {
		assertThrows(DynamicDynamoServiceException.class, () -> service.getUploadUserByFileId("tbl", null));
		assertThrows(DynamicDynamoServiceException.class, () -> service.getUploadUserByFileId("tbl", "   "));
	}

	@Test
	void wrapsSdkException_fromDynamo() {
		when(dynamoDbClient.scan(any(ScanRequest.class))).thenThrow(SdkException.create("boom", null));
		assertThrows(DynamicDynamoServiceException.class, () -> service.getUploadUserByFileId("tbl", "file-x"));
	}
}

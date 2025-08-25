package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicDynamoServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
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
}

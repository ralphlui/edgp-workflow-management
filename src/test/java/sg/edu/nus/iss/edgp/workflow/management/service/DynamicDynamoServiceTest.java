package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
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
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

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
}

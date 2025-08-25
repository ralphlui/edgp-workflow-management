package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

@ExtendWith(MockitoExtension.class)
public class DynamicDynamoServiceTest {

	@Mock
	private DynamoDbClient dynamoDbClient;

	@InjectMocks
	private DynamicDynamoService service;

	@BeforeEach
	void setUp() {
		service = new DynamicDynamoService(dynamoDbClient);
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
}

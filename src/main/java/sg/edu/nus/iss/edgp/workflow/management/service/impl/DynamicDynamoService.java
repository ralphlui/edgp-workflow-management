package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.FileStatus;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicDynamoServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.IDynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.utility.FileMetricsConstants;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@RequiredArgsConstructor
@Service
public class DynamicDynamoService implements IDynamicDynamoService {

	private final DynamoDbClient dynamoDbClient;
	private static final Logger logger = LoggerFactory.getLogger(DynamicDynamoService.class);

	@Override
	public boolean tableExists(String tableName) {
		try {
			dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
			return true;
		} catch (ResourceNotFoundException e) {
			return false;
		}
	}

	@Override
	public void createTable(String tableName) {
		CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
				.keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
				.attributeDefinitions(
						AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
				.billingMode(BillingMode.PAY_PER_REQUEST).build();

		dynamoDbClient.createTable(request);
		// Wait until table is ACTIVE
		waitForTableToBecomeActive(tableName);
	}

	private void waitForTableToBecomeActive(String tableName) {
		while (true) {
			DescribeTableResponse response = dynamoDbClient
					.describeTable(DescribeTableRequest.builder().tableName(tableName).build());

			String status = response.table().tableStatusAsString();
			if ("ACTIVE".equalsIgnoreCase(status))
				break;

			try {
				Thread.sleep(1000); // Wait 1 sec before checking again
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException("Interrupted while waiting for DynamoDB table to become active");
			}
		}
	}

	@Override
	public void insertWorkFlowStatusData(String tableName, Map<String, String> rawData) {
		if (rawData == null || rawData.isEmpty()) {
			throw new IllegalArgumentException("No data provided for insert.");
		}
		try {

			Map<String, AttributeValue> item = new HashMap<>();
			if (!rawData.containsKey("id")) {
				item.put("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
			} else {
				item.put("id", AttributeValue.builder().s(rawData.get("id")).build());
			}

			for (Map.Entry<String, String> entry : rawData.entrySet()) {
				String column = entry.getKey();
				String value = entry.getValue();

				if (column == null || column.trim().isEmpty())
					continue;

				AttributeValue attrVal = convertToAttributeValue(value);
				item.put(column, attrVal);
			}

			PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(item).build();

			dynamoDbClient.putItem(request);

		} catch (Exception ex) {
			logger.error("An error occurred while inserting workflow status data to db.... {}", ex);
			throw new DynamicDynamoServiceException("Interrupted while inserting workflow status data to db", ex);
		}

	}

	private AttributeValue convertToAttributeValue(String value) {
		if (value == null || value.trim().isEmpty()) {
			return AttributeValue.builder().nul(true).build();
		}

		String trimmed = value.trim();

		try {
			// Numeric detection
			if (trimmed.matches("-?\\d+")) {
				return AttributeValue.builder().n(trimmed).build(); // integer
			} else if (trimmed.matches("-?\\d+\\.\\d+")) {
				return AttributeValue.builder().n(trimmed).build(); // decimal
			} else if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
				return AttributeValue.builder().bool(Boolean.parseBoolean(trimmed)).build();
			} else {
				return AttributeValue.builder().s(trimmed).build(); // default to string
			}
		} catch (Exception e) {
			return AttributeValue.builder().s(trimmed).build(); // fallback
		}
	}

	@Override
	public Map<String, AttributeValue> getFileStatusDataByFileId(String tableName, String fileId) {

		try {
			Map<String, AttributeValue> expressionValues = new HashMap<>();
			expressionValues.put(":fileId", AttributeValue.builder().s(fileId).build());

			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("fileId = :fileId")
					.expressionAttributeValues(expressionValues).build();

			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

			if (results.isEmpty()) {
				return null;
			}

			return results.get(0);

		} catch (Exception ex) {
			logger.error("An error occurred while retireving file status data by file id.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retireving file status data by file id",
					ex);
		}

	}

	@Override
	public void insertFileStatusData(String tableName, Map<String, String> rawData) {
		if (rawData == null || rawData.isEmpty()) {
			throw new IllegalArgumentException("No data provided for insert.");
		}

		try {
			Map<String, AttributeValue> item = new HashMap<>();

			if (!rawData.containsKey("id")) {
				item.put("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
			} else {
				item.put("id", AttributeValue.builder().s(rawData.get("id")).build());
			}

			for (Map.Entry<String, String> entry : rawData.entrySet()) {
				String column = entry.getKey();
				String value = entry.getValue();

				if (column == null || column.trim().isEmpty())
					continue;

				AttributeValue attrVal = convertToAttributeValue(value);
				item.put(column, attrVal);
			}

			PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(item).build();

			dynamoDbClient.putItem(request);

		} catch (Exception ex) {
			logger.error("An error occurred while inserting file status data to db.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while inserting file status data to db", ex);
		}
	}

	@Override
	public void updateFileStatus(String tableName, FileStatus fileStatus) {
		Map<String, AttributeValue> key = new HashMap<>();
		try {
			key.put("id", AttributeValue.builder().s(fileStatus.getId()).build());

			Map<String, AttributeValueUpdate> updates = new HashMap<>();
			updates.put(FileMetricsConstants.SUCCESS_COUNT,
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(fileStatus.getSuccessCount()).build())
							.action(AttributeAction.PUT).build());

			updates.put(FileMetricsConstants.REJECTED_COUNT,
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(fileStatus.getRejectedCount()).build())
							.action(AttributeAction.PUT).build());

			updates.put(FileMetricsConstants.FAILED_COUNT,
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(fileStatus.getFailedCount()).build())
							.action(AttributeAction.PUT).build());

			updates.put(FileMetricsConstants.QUARANTINED_COUNT,
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(fileStatus.getQuarantinedCount()).build())
							.action(AttributeAction.PUT).build());

			updates.put(FileMetricsConstants.PROCESSED_COUNT,
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(fileStatus.getProcessedCount()).build())
							.action(AttributeAction.PUT).build());

			UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
					.attributeUpdates(updates).build();

			dynamoDbClient.updateItem(updateRequest);

		} catch (Exception ex) {
			logger.error("An error occurred while updating file status.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while updating file status", ex);
		}

	}

	@Override
	public Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id) {

		try {

			Map<String, AttributeValue> expressionValues = new HashMap<>();
			expressionValues.put(":id", AttributeValue.builder().s(id).build());

			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("id = :id")
					.expressionAttributeValues(expressionValues).build();

			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

			if (results.isEmpty()) {
				return null;
			}

			return results.get(0);

		} catch (Exception ex) {
			logger.error("An error occurred while retireving data by workflow status id.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retireving data by workflow status id",
					ex);
		}
	}

	@Override
	public void updateWorkflowStatus(String tableName, WorkflowStatus workflowStatus) {

		try {

			Map<String, AttributeValue> key = new HashMap<>();
			key.put("id", AttributeValue.builder().s(workflowStatus.getId()).build());

			Map<String, AttributeValueUpdate> updates = new HashMap<>();
			updates.put("ruleStatus",
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(workflowStatus.getRuleStatus()).build())
							.action(AttributeAction.PUT).build());

			updates.put("finalStatus",
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(workflowStatus.getFinalStatus()).build())
							.action(AttributeAction.PUT).build());

			updates.put("message",
					AttributeValueUpdate.builder()
							.value(AttributeValue.builder().s(workflowStatus.getMessage()).build())
							.action(AttributeAction.PUT).build());

			UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
					.attributeUpdates(updates).build();

			dynamoDbClient.updateItem(updateRequest);

		} catch (Exception ex) {
			logger.error("An error occurred while updating workflow status.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while updating workflow status", ex);
		}
	}

	@Override
	public Map<String, Object> retrieveDataList(String tableName, String fileId, String status,
			SearchRequest searchRequest) {

		try {
			Map<String, AttributeValue> expressionValues = new HashMap<>();
			List<String> filterConditions = new ArrayList<>();

			if (fileId != null && !fileId.isEmpty()) {
				filterConditions.add("fileId = :fileId");
				expressionValues.put(":fileId", AttributeValue.builder().s(fileId).build());
			}

			if (status != null && !status.isEmpty()) {
				filterConditions.add("finalStatus = :finalStatus");
				expressionValues.put(":finalStatus", AttributeValue.builder().s(status).build());
			}

			Map<String, AttributeValue> lastEvaluatedKey = null;

			List<Map<String, AttributeValue>> allFilteredItems = new ArrayList<>();

// Common scan loop (used for both paginated and non-paginated)
			do {
				ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(tableName).limit(50); // Optional scan
																										// page size

				if (!filterConditions.isEmpty()) {
					scanBuilder.filterExpression(String.join(" AND ", filterConditions))
							.expressionAttributeValues(expressionValues);
				}

				if (lastEvaluatedKey != null) {
					scanBuilder.exclusiveStartKey(lastEvaluatedKey);
				}

				ScanResponse response = dynamoDbClient.scan(scanBuilder.build());
				allFilteredItems.addAll(response.items());
				lastEvaluatedKey = response.lastEvaluatedKey();

			} while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());


			allFilteredItems.sort(Comparator.comparing(item -> item.get("id").s()));

			Map<String, Object> result = new HashMap<>();
			result.put("totalCount", allFilteredItems.size());

// Case 1: No pagination – return all items with total
			if (searchRequest.getPage() == null || searchRequest.getSize() == null) {
				result.put("items", allFilteredItems);
				return result;
			}

// Case 2: Paginated result
			int size = searchRequest.getSize();
			int page = searchRequest.getPage();
			int fromIndex = (page - 1) * size;
			int toIndex = fromIndex + size;

			if (fromIndex >= allFilteredItems.size()) {
				result.put("items", Collections.emptyList());
				return result;
			}

			List<Map<String, AttributeValue>> paginatedItems = allFilteredItems.subList(fromIndex,
					Math.min(toIndex, allFilteredItems.size()));

			result.put("items", paginatedItems);
			return result;

		} catch (Exception ex) {
			logger.error("An error occurred while retrieving data list.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retrieving data list", ex);
		}
	}
}

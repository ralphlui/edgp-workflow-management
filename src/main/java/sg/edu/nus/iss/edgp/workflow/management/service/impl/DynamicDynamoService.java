package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicDynamoServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.IDynamicDynamoService;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
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
	public Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id) {

		try {

			if (id == null || id.isEmpty()) {
				throw new DynamicDynamoServiceException(
						"Workflow status id is empty while  retireving workflow status data.");
			}

			Map<String, AttributeValue> expressionValues = new HashMap<>();
			expressionValues.put(":id", AttributeValue.builder().s(id).build());

			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("id = :id")
					.expressionAttributeValues(expressionValues).build();

			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

			if (results.isEmpty()) {
				return null;
			}

			logger.info("Successfully retrieve work flow status data by workflow id");
			return results.get(0);

		} catch (Exception ex) {
			logger.error("An error occurred while retireving data by workflow status id.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retireving data by workflow status id",
					ex);
		}
	}
	
	@Override
	public Map<String, AttributeValue> getFileDataByFileId(String tableName, String id) {

		try {

			if (id == null || id.isEmpty()) {
				throw new DynamicDynamoServiceException(
						"File id is empty while retireving file data by file Id.");
			}

			Map<String, AttributeValue> expressionValues = new HashMap<>();
			expressionValues.put(":id", AttributeValue.builder().s(id).build());

			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("id = :id")
					.expressionAttributeValues(expressionValues).build();

			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

			if (results.isEmpty()) {
				return null;
			}

			logger.info("Successfully retrieve file data by file id.");
			return results.get(0);

		} catch (Exception ex) {
			logger.error("An error occurred while retireving file data by file id.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retireving file data by file id",
					ex);
		}
	}

	@Override
	public void updateWorkflowStatus(String tableName, WorkflowStatus workflowStatus) {
		try {
			Map<String, AttributeValue> key = new HashMap<>();
			key.put("id", AttributeValue.builder().s(workflowStatus.getId()).build());

			StringBuilder set = new StringBuilder();
			Map<String, String> names = new HashMap<>();
			Map<String, AttributeValue> values = new HashMap<>();
			int n = 0;

			if (workflowStatus.getRuleStatus() != null) {
				if (n++ > 0)
					set.append(", ");
				names.put("#rs", "rule_status");
				values.put(":rs", AttributeValue.builder().s(workflowStatus.getRuleStatus()).build());
				set.append("#rs = :rs");
			}

			if (workflowStatus.getFinalStatus() != null) {
				if (n++ > 0)
					set.append(", ");
				names.put("#fs", "final_status");
				values.put(":fs", AttributeValue.builder().s(workflowStatus.getFinalStatus()).build());
				set.append("#fs = :fs");
			}

			if (workflowStatus.getFailedValidations() != null) {
				if (n++ > 0)
					set.append(", ");
				names.put("#fv", "failed_validations");

				List<AttributeValue> fvList = new ArrayList<>();
				for (Map<String, Object> item : workflowStatus.getFailedValidations()) {
					fvList.add(AttributeValue.builder().m(toAvMap(item)).build());
				}
				values.put(":fv", AttributeValue.builder().l(fvList).build());
				values.put(":empty", AttributeValue.builder().l(Collections.emptyList()).build());

				// Always create list if missing, then append
				set.append("#fv = list_append(if_not_exists(#fv, :empty), :fv)");
			}
			if (n == 0)
				return;

			UpdateItemRequest req = UpdateItemRequest.builder().tableName(tableName).key(key)
					.updateExpression("SET " + set).expressionAttributeNames(names).expressionAttributeValues(values)
					.returnValues(ReturnValue.ALL_NEW).build();

			dynamoDbClient.updateItem(req);

		} catch (Exception ex) {
			logger.error("Failed to update workflow status", ex);
			throw new DynamicDynamoServiceException("Error updating workflow status", ex);
		}
	}

	private Map<String, AttributeValue> toAvMap(Map<String, Object> map) {
		Map<String, AttributeValue> out = new HashMap<>();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			Object v = e.getValue();
			if (v == null)
				continue; // DynamoDB doesn't store nulls
			out.put(e.getKey(), toAv(v));
		}
		return out;
	}

	private AttributeValue toAv(Object v) {
		if (v instanceof String s)
			return AttributeValue.builder().s(s).build();
		if (v instanceof Number n)
			return AttributeValue.builder().n(n.toString()).build();
		if (v instanceof Boolean b)
			return AttributeValue.builder().bool(b).build();
		if (v instanceof byte[] b)
			return AttributeValue.builder().b(SdkBytes.fromByteArray(b)).build();
		if (v instanceof List<?> list) {
			List<AttributeValue> l = new ArrayList<>();
			for (Object o : list)
				l.add(toAv(o));
			return AttributeValue.builder().l(l).build();
		}
		if (v instanceof Map<?, ?> m) {
			Map<String, AttributeValue> mm = new HashMap<>();
			for (Map.Entry<?, ?> me : m.entrySet()) {
				Object key = me.getKey();
				Object val = me.getValue();
				if (key != null && val != null)
					mm.put(key.toString(), toAv(val));
			}
			return AttributeValue.builder().m(mm).build();
		}
		// Fallback: store as string
		return AttributeValue.builder().s(v.toString()).build();
	}

	@Override
	public Map<String, Object> retrieveDataList(String tableName, String fileId,
			SearchRequest searchRequest, String userOrgId) {

		try {
			Map<String, AttributeValue> expressionValues = new HashMap<>();
			List<String> filterConditions = new ArrayList<>();
			
			filterConditions.add("organization_id = :organization_id");
			expressionValues.put(":organization_id", AttributeValue.builder().s(userOrgId).build());

			if (fileId != null && !fileId.isEmpty()) {
				filterConditions.add("file_id = :file_id");
				expressionValues.put(":file_id", AttributeValue.builder().s(fileId).build());
			}

			if (searchRequest.getStatus() != null && !searchRequest.getStatus().isEmpty()) {
				filterConditions.add("final_status = :final_status");
				expressionValues.put(":final_status", AttributeValue.builder().s(searchRequest.getStatus()).build());
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
			logger.info("Successfully retrieve work flow status data list.");
			return result;

		} catch (Exception ex) {
			logger.error("An error occurred while retrieving data list.... {}", ex);
			throw new DynamicDynamoServiceException("An error occurred while retrieving data list", ex);
		}
	}
}

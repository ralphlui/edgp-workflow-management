package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
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

	public void waitForTableToBecomeActive(String tableName) {
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
//
//	@Override
//	public Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id) {
//
//		try {
//
//			if (id == null || id.isEmpty()) {
//				throw new DynamicDynamoServiceException(
//						"Workflow status id is empty while  retireving workflow status data.");
//			}
//
//			Map<String, AttributeValue> expressionValues = new HashMap<>();
//			expressionValues.put(":id", AttributeValue.builder().s(id).build());
//
//			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("id = :id")
//					.expressionAttributeValues(expressionValues).build();
//
//			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();
//
//			if (results.isEmpty()) {
//				return null;
//			}
//
//			logger.info("Successfully retrieve work flow status data by workflow id");
//			return results.get(0);
//
//		} catch (Exception ex) {
//			logger.error("An error occurred while retireving data by workflow status id.... {}", ex);
//			throw new DynamicDynamoServiceException("An error occurred while retireving data by workflow status id",
//					ex);
//		}
//	}
	
//	@Override
//	public Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id) {
//	    try {
//	        if (id == null || id.isEmpty()) {
//	            throw new DynamicDynamoServiceException(
//	                "Workflow status id is empty while retrieving workflow status data.");
//	        }
//
//	        // Build the key for partition key lookup
//	        Map<String, AttributeValue> key = new HashMap<>();
//	        key.put("id", AttributeValue.builder().s(id).build());
//
//	        GetItemRequest getItemRequest = GetItemRequest.builder()
//	            .tableName(tableName)
//	            .key(key)
//	            .consistentRead(true)  // Optional: ensures you get the most recent data
//	            .build();
//
//	        GetItemResponse response = dynamoDbClient.getItem(getItemRequest);
//
//	        if (!response.hasItem()) {
//	            logger.warn("No workflow status data found for id: {}", id);
//	            return null;
//	        }
//
//	        logger.info("Successfully retrieved workflow status data by workflow id: {}", id);
//	        return response.item();
//
//	    } catch (DynamicDynamoServiceException ex) {
//	        logger.error("DynamoDB error while retrieving data by workflow status id: {}", id, ex);
//	        throw new DynamicDynamoServiceException(
//	            "An error occurred while retrieving data by workflow status id", ex);
//	    } catch (Exception ex) {
//	        logger.error("Unexpected error while retrieving data by workflow status id: {}", id, ex);
//	        throw new DynamicDynamoServiceException(
//	            "An error occurred while retrieving data by workflow status id", ex);
//	    }
//	}
	
	
	@Override
	public Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id) {
	    try {
	        if (id == null || id.isEmpty()) {
	            throw new DynamicDynamoServiceException(
	                "Workflow status id is empty while retrieving workflow status data.");
	        }

	        QueryRequest queryRequest = QueryRequest.builder()
	            .tableName(tableName)
	            .keyConditionExpression("id = :id")
	            .expressionAttributeValues(Collections.singletonMap(
	                ":id", AttributeValue.builder().s(id).build()))
	            .build();

	        QueryResponse response = dynamoDbClient.query(queryRequest);
	        List<Map<String, AttributeValue>> results = response.items();

	        if (results.isEmpty()) {
	            logger.warn("No workflow status data found for id: {}", id);
	            return null;
	        }

	        logger.info("Successfully retrieved workflow status data by workflow id: {}", id);
	        return results.get(0);

	    } catch (DynamicDynamoServiceException ex) {
	        logger.error("DynamoDB error while retrieving data by workflow status id: {}", id, ex);
	        throw new DynamicDynamoServiceException(
	            "An error occurred while retrieving data by workflow status id", ex);
	    } catch (Exception ex) {
	        logger.error("Unexpected error while retrieving data by workflow status id: {}", id, ex);
	        throw new DynamicDynamoServiceException(
	            "An error occurred while retrieving data by workflow status id", ex);
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
			
			if (workflowStatus.getDataQualityStatus() != null) {
			    if (n++ > 0) set.append(", ");
			    names.put("#ds", "dataquality_status");
			    values.put(":ds", AttributeValue.builder()
			            .s(workflowStatus.getDataQualityStatus())
			            .build());
			    set.append("#ds = :ds");   // <-- use the alias here
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
	public Map<String, Object> retrieveDataList(String tableName, String fileId, SearchRequest searchRequest,
			String userOrgId) {

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
				expressionValues.put(":final_status", AttributeValue.builder().s(searchRequest.getStatus().toLowerCase()).build());
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

	@Override
	public File exportToCsv(String tableName, HashMap<String, String> fileInfo) {
	    File tempFile = null;
	    try {
	        if (fileInfo == null || fileInfo.isEmpty()) {
	            throw new IllegalArgumentException("File info must not be null or empty.");
	        }
	        final String fileId = Objects.requireNonNull(fileInfo.get("id"), "id is required").trim();
	        final String fileName = Objects.requireNonNull(fileInfo.get("name"), "name is required").trim();

	        Map<String, AttributeValue> expressionValues = new HashMap<>();
	        expressionValues.put(":file_id", AttributeValue.builder().s(fileId).build());

	        ScanRequest scanRequest = ScanRequest.builder()
	                .tableName(tableName)
	                .filterExpression("file_id = :file_id")
	                .expressionAttributeValues(expressionValues)
	                .build();

	        ScanResponse response = dynamoDbClient.scan(scanRequest);
	        List<Map<String, AttributeValue>> items = response.items();

	        if (items == null || items.isEmpty()) {
	            throw new IllegalStateException("No data found to export.");
	        }

	        Set<String> excludedFields = new HashSet<>(Arrays.asList(
	            "id","domain_name","file_id","organization_id",
	            "policy_id","uploaded_by","created_date",
	            "failed_validations",  
	            "final_status" 
	        ));

	       
	        Set<String> headers = new LinkedHashSet<>(items.get(0).keySet());
	        for (Map<String, AttributeValue> it : items) headers.addAll(it.keySet());
	        headers.removeAll(excludedFields);

	        headers.add("failed_rule_names");
	        headers.add("failed_column_names");
	        headers.add("failed_error_messages");
	        headers.add("failed_statuses");
	        headers.add("final_status");

	        tempFile = Files.createTempFile(fileName.replace(".csv", "") + "-result-", ".csv").toFile();

	        try (PrintWriter writer = new PrintWriter(new FileWriter(tempFile))) {
	            writer.println(String.join(",", headers));

	            for (Map<String, AttributeValue> item : items) {
	                
	                List<Map<String, AttributeValue>> failedList = new ArrayList<>();

	                AttributeValue topFv = item.get("failed_validations");
	                if (topFv != null && topFv.hasL()) {
	                    for (AttributeValue av : topFv.l()) if (av != null && av.hasM()) failedList.add(av.m());
	                } else {
	                    AttributeValue ruleStatus = item.get("rule_status");
	                    if (ruleStatus != null && ruleStatus.hasM()) {
	                        AttributeValue nested = ruleStatus.m().get("failed_validations");
	                        if (nested != null && nested.hasL()) {
	                            for (AttributeValue av : nested.l()) if (av != null && av.hasM()) failedList.add(av.m());
	                        }
	                    }
	                }

	                List<String> ruleNames     = new ArrayList<>();
	                List<String> columnNames   = new ArrayList<>();
	                List<String> errorMessages = new ArrayList<>();
	                List<String> statuses      = new ArrayList<>();

	                for (Map<String, AttributeValue> m : failedList) {
	                    ruleNames.add(safeS(m.get("rule_name")));
	                    columnNames.add(safeS(m.get("column_name")));
	                    errorMessages.add(safeS(m.get("error_message")));
	                    statuses.add(safeS(m.get("status")));
	                }

	                List<String> row = new ArrayList<>(headers.size());
	                for (String h : headers) {
	                    switch (h) {
	                        case "failed_rule_names":
	                            row.add(csvEscape(String.join(";", ruleNames)));
	                            break;
	                        case "failed_column_names":
	                            row.add(csvEscape(String.join(";", columnNames)));
	                            break;
	                        case "failed_error_messages":
	                            row.add(csvEscape(String.join(";", errorMessages)));
	                            break;
	                        case "failed_statuses":
	                            row.add(csvEscape(String.join(";", statuses)));
	                            break;
	                        case "final_status":
	                           
	                            row.add(csvEscape(attrToFlatString(item.get("final_status"))));
	                            break;
	                        default:
	                            row.add(csvEscape(attrToFlatString(item.get(h))));
	                    }
	                }

	                writer.println(String.join(",", row));
	            }
	        }

	        return tempFile;

	    } catch (Exception ex) {
	        logger.error("An error occurred while exporting data list.", ex);
	        throw new DynamicDynamoServiceException("An error occurred while exporting data list", ex);
	    }
	}



	private String safeS(AttributeValue v) {
	    return (v != null && v.s() != null) ? v.s() : "";
	}

	private String attrToFlatString(AttributeValue val) {
	    if (val == null) return "";
	    if (val.s() != null) return val.s();
	    if (val.n() != null) return val.n();
	    if (val.bool() != null) return String.valueOf(val.bool());
	    if (val.hasSs()) return String.join(";", val.ss());
	    if (val.hasNs()) return String.join(";", val.ns());
	   
	    if (val.hasL()) return "[" + val.l().stream().map(this::toJsonValue).collect(java.util.stream.Collectors.joining(",")) + "]";
	    if (val.hasM()) {
	        String body = val.m().entrySet().stream()
	                .map(e -> "\"" + jsonEscape(e.getKey()) + "\":" + toJsonValue(e.getValue()))
	                .collect(java.util.stream.Collectors.joining(","));
	        return "{" + body + "}";
	    }
	    return "";
	}

	private String toJsonValue(AttributeValue v) {
	    if (v == null) return "null";
	    if (v.s() != null) return "\"" + jsonEscape(v.s()) + "\"";
	    if (v.n() != null) return v.n();
	    if (v.bool() != null) return String.valueOf(v.bool());
	    if (v.hasSs()) return v.ss().stream().map(s -> "\"" + jsonEscape(s) + "\"").collect(java.util.stream.Collectors.joining(",", "[", "]"));
	    if (v.hasNs()) return v.ns().stream().collect(java.util.stream.Collectors.joining(",", "[", "]"));
	    if (v.hasL()) return v.l().stream().map(this::toJsonValue).collect(java.util.stream.Collectors.joining(",", "[", "]"));
	    if (v.hasM()) {
	        String body = v.m().entrySet().stream()
	                .map(e -> "\"" + jsonEscape(e.getKey()) + "\":" + toJsonValue(e.getValue()))
	                .collect(java.util.stream.Collectors.joining(","));
	        return "{" + body + "}";
	    }
	    return "null";
	}

	private String csvEscape(String s) {
	    if (s == null) return "";
	    boolean needsQuotes = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
	    String cleaned = s.replace("\"", "\"\"").replace("\r", " ").replace("\n", " ");
	    return needsQuotes ? "\"" + cleaned + "\"" : cleaned;
	}

	private String jsonEscape(String s) {
	    if (s == null) return "";
	    StringBuilder sb = new StringBuilder(s.length() + 8);
	    for (char c : s.toCharArray()) {
	        switch (c) {
	            case '\"': sb.append("\\\""); break;
	            case '\\': sb.append("\\\\"); break;
	            case '\b': sb.append("\\b"); break;
	            case '\f': sb.append("\\f"); break;
	            case '\n': sb.append("\\n"); break;
	            case '\r': sb.append("\\r"); break;
	            case '\t': sb.append("\\t"); break;
	            default: if (c < 0x20) sb.append(String.format("\\u%04x", (int)c)); else sb.append(c);
	        }
	    }
	    return sb.toString();
	}


	@Override
	public String getUploadUserByFileId(String tableName, String id) {
		try {
			if (id == null || id.trim().isEmpty()) {
				throw new DynamicDynamoServiceException("File ID is empty while retrieving uploaded user.");
			}

			Map<String, AttributeValue> expressionValues = new HashMap<>();
			expressionValues.put(":id", AttributeValue.builder().s(id).build());

			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("id = :id")
					.expressionAttributeValues(expressionValues).build();

			List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

			if (results.isEmpty()) {
				return null;
			}

			Map<String, AttributeValue> item = results.get(0);

			AttributeValue uploadedByAttr = item.get("uploaded_by");
			return (uploadedByAttr != null) ? uploadedByAttr.s() : null;

		} catch (Exception ex) {
			logger.error("An error occurred while retrieving data by file ID: {}", ex.getMessage(), ex);
			throw new DynamicDynamoServiceException("An error occurred while retrieving data by file ID", ex);
		}
	}

}

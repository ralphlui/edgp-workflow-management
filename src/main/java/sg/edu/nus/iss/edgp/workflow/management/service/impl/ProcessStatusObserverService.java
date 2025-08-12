package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.IProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverService implements IProcessStatusObserverService {

	private final DynamoDbClient dynamoDbClient;

	@Override
	public String fetchOldestIdByProcessStage(FileProcessStage stage) {
		ScanRequest req = ScanRequest.builder().tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim())
				.filterExpression("#ps = :ps").expressionAttributeNames(Map.of("#ps", "process_stage"))
				.expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(stage.name()).build()))
				.projectionExpression("id, uploaded_date").build();

		String fileId = null;
		String fileCreated = null;

		for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
			for (Map<String, AttributeValue> item : page.items()) {
				AttributeValue id = item.get("id");
				AttributeValue created = item.get("uploaded_date");
				if (id == null || id.s() == null || id.s().isBlank())
					continue;
				if (created == null || created.s() == null || created.s().isBlank())
					continue;

				String cd = created.s();
				if (fileCreated == null || cd.compareTo(fileCreated) < 0) {
					fileCreated = cd;
					fileId = id.s();
				}
			}
		}
		return fileId;
	}

	@Override
	public boolean isAllDataProcessed(String fileId) {

		ScanRequest scanRequest = ScanRequest.builder().tableName(DynamoConstants.MASTER_DATA_TASK_TRACKER_TABLE_NAME.trim()).filterExpression("file_id = :fid")
				.expressionAttributeValues(Map.of(":fid", AttributeValue.builder().s(fileId).build())).build();

		List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

		// Return false if any item has null or empty file_status
		return results.stream().allMatch(item -> {
			AttributeValue statusAttr = item.get("final_status");
			return statusAttr != null && statusAttr.s() != null && !statusAttr.s().isBlank();
		});

	}

	@Override
	public boolean isAllTrueForFile(String fileId) {
		ScanRequest req = ScanRequest.builder().tableName(DynamoConstants.MASTER_DATA_TASK_TRACKER_TABLE_NAME.trim())
				.filterExpression("#fid = :fid AND #fs = :false")
				.expressionAttributeNames(Map.of("#fid", "file_id", "#fs", "final_status"))
				.expressionAttributeValues(Map.of(":fid", AttributeValue.builder().s(fileId).build(), ":false",
						AttributeValue.builder().s("false").build()))
				.select(Select.COUNT).build();

		for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
			if (page.count() > 0)
				return false;
		}
		return true;
	}

	@Override
	public void updateFileStageAndStatus(String fileId, FileProcessStage stage, boolean status) {
		Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s(fileId).build());

		UpdateItemRequest req = UpdateItemRequest.builder()
				.tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim()).key(key)
				.updateExpression("SET #stage = :stage, #status = :status, updated_date = :now")
				.expressionAttributeNames(Map.of("#stage", "process_stage", "#status", "file_status"))
				.expressionAttributeValues(Map.of(":stage", AttributeValue.builder().s(stage.name()).build(), ":status",
						AttributeValue.builder().bool(status).build(), ":now",
						AttributeValue.builder().s(java.time.Instant.now().toString()).build()))
				.conditionExpression("attribute_exists(id)").returnValues(ReturnValue.UPDATED_NEW).build();

		dynamoDbClient.updateItem(req);
	}

}

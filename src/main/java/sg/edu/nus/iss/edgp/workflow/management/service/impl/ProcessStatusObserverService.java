package sg.edu.nus.iss.edgp.workflow.management.service.impl;

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
public class ProcessStatusObserverService implements IProcessStatusObserverService{
	
	private final DynamoDbClient dynamoDbClient;
	
	@Override
	public String fetchOldestIdByProcessStage(FileProcessStage stage) {
	    ScanRequest req = ScanRequest.builder()
	        .tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim())
	        .filterExpression("#ps = :ps")
	        .expressionAttributeNames(Map.of("#ps", "process_stage"))
	        .expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(stage.name()).build()))
	        .projectionExpression("id, created_date")
	        .build();

	    String fileId = null;
	    String fileCreated = null;

	    for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
	        for (Map<String, AttributeValue> item : page.items()) {
	            AttributeValue id = item.get("id");
	            AttributeValue created = item.get("created_date");
	            if (id == null || id.s() == null || id.s().isBlank()) continue;
	            if (created == null || created.s() == null || created.s().isBlank()) continue;

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
	public boolean isFileProcessed(String fileId) {
	    QueryRequest req = QueryRequest.builder()
	        .tableName(DynamoConstants.WORK_FLOW_STATUS.trim())
	        .keyConditionExpression("file_id = :fid")
	        .filterExpression("attribute_not_exists(final_status)")
	        .expressionAttributeValues(Map.of(":fid", AttributeValue.builder().s(fileId).build()))
	        .select(Select.COUNT)
	        .build();

	    return dynamoDbClient.query(req).count() == 0;
	}
	
	@Override
	public void updateFileStage(String fileId, FileProcessStage processStage) {

		Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s(fileId).build());

		UpdateItemRequest req = UpdateItemRequest.builder()
				.tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim()).key(key)
				.updateExpression("SET #ps = :ps, updated_at = :now")
				.expressionAttributeNames(Map.of("#ps", "process_stage"))
				.expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(processStage.name()).build(),
						 ":now",
						AttributeValue.builder().s(java.time.Instant.now().toString()).build()))
				.conditionExpression("attribute_exists(id)").returnValues(ReturnValue.UPDATED_NEW).build();

		dynamoDbClient.updateItem(req);
	}

}

package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.IProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverService implements IProcessStatusObserverService{
	
	private final DynamoDbClient dynamoDbClient;
	
	@Override
	public Optional<String> fetchOldestIdByProcessStage(FileProcessStage stage) {
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
	    return Optional.ofNullable(fileId);
	}
	
	@Override
	public boolean isFileProcessed(String fileId) {
	    Map<String, AttributeValue> eav = Map.of(
	        ":fid", AttributeValue.builder().s(fileId).build(),
	        ":zero", AttributeValue.builder().n("0").build()
	    );

	    ScanRequest req = ScanRequest.builder()
	        .tableName(DynamoConstants.WORK_FLOW_STATUS.trim())
	        .filterExpression("file_id = :fid AND (attribute_not_exists(final_status) OR size(final_status) = :zero)")
	        .expressionAttributeValues(eav)
	        .projectionExpression("file_id")
	        .build();

	    for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
	        if (page.count() > 0) return false;
	    }
	    return true;
	}


}

package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Map;

import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IDynamicDynamoService {

	boolean tableExists(String tableName);

	void createTable(String tableName);

	Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id);

	void updateWorkflowStatus(String tableName, WorkflowStatus workflowStatus);
	

	Map<String, Object> retrieveDataList(String tableName, String fileId,
			SearchRequest searchRequest, String userOrgId);
}

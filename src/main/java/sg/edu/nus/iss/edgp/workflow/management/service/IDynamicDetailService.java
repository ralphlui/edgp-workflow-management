package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Map;

import sg.edu.nus.iss.edgp.workflow.management.dto.FileStatus;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IDynamicDetailService {

	boolean tableExists(String tableName);

	void createTable(String tableName);

	void insertWorkFlowStatusData(String tableName, Map<String, String> rawData);

	Map<String, AttributeValue> getFileStatusDataByFileId(String tableName, String fileId);

	void insertFileStatusData(String tableName, Map<String, String> rawData);

	void updateFileStatus(String tableName, FileStatus fileStatus);

	Map<String, AttributeValue> getDataByWorkflowStatusId(String tableName, String id);

	void updateWorkflowStatus(String tableName, WorkflowStatus workflowStatus);
	
	boolean isFileProcessed(String fileId);

}

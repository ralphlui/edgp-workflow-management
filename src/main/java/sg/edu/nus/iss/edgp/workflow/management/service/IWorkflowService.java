package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.List;
import java.util.Map;

import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;

public interface IWorkflowService {

	void updateWorkflowStatus(Map<String, Object> data);
	
	boolean isAllDataProcessed(String fileId);
 
	List<Map<String, Object>> retrieveDataList(String fileId, SearchRequest searchRequest, String userOrdId);
	
	Map<String, Object> retrieveDataRecordDetailbyWorkflowId(String workflowStatusId);
 
}

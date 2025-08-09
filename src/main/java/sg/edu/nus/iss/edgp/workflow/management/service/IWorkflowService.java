package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.List;
import java.util.Map;

import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;

public interface IWorkflowService {

	void updateWorkflowStatus(Map<String, Object> data);
	
	List<Map<String, Object>> retrieveDataList(String fileId, String status, SearchRequest searchRequest);
}

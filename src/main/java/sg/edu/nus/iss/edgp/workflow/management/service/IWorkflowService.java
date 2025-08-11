package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Map;

public interface IWorkflowService {

	void updateWorkflowStatus(Map<String, Object> data);
	
	boolean isFileProcessed(String fileId);
}

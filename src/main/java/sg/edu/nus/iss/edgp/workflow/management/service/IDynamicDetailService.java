package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Map;

public interface IDynamicDetailService {

	boolean tableExists(String tableName);
	
	void createTable(String tableName);
	
	void insertWorkFlowStatusData(String tableName, Map<String, String> rawData);
}

package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Map;

public interface IDynamicSQLService {

	void buildCreateTableSQL(Map<String, Object> data, String tableName);

	void insertData(String tableName, Map<String, Object> data);
}

package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.Iterator;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.service.IDynamicSQLService;

@RequiredArgsConstructor
@Service
public class DynamicSQLService implements IDynamicSQLService {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void buildCreateTableSQL(Map<String, Object> data) {

		StringBuilder result = new StringBuilder();
		String tableName = (String) data.get("category");

		Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<String, Object> entry = iterator.next();
			String columnName = entry.getKey().toLowerCase();
			Object value = entry.getValue();
			String columnType = (value != null) ? mapDataType(value.getClass()) : "VARCHAR(255)";

			result.append("`").append(columnName).append("` ").append(columnType);

			if (iterator.hasNext()) {
				result.append(", ");
			}
		}

		String columnDefs = result.toString();

		String staticColumns = String.join(", ", "`id` VARCHAR(36) PRIMARY KEY",
				"`created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
				"`updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");

		String query = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" + staticColumns + ", " + columnDefs + ")";
		jdbcTemplate.execute(query);
	}

	private static String mapDataType(Class<?> clazz) {
		if (clazz == Integer.class)
			return "INT";
		if (clazz == String.class)
			return "VARCHAR(255)";
		if (clazz == Double.class)
			return "DOUBLE";
		if (clazz == Boolean.class)
			return "BOOLEAN";
		if (clazz == java.util.Date.class)
			return "DATETIME";
		return "VARCHAR(255)";
	}
}

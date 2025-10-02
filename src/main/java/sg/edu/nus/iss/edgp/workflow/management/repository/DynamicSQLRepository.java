package sg.edu.nus.iss.edgp.workflow.management.repository;

import java.sql.Connection;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;


@Repository
public class DynamicSQLRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	private static final Logger logger = LoggerFactory.getLogger(DynamicSQLRepository.class);

	public boolean tableExists(String schema, String tableName) {
		String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
		Integer count = jdbcTemplate.queryForObject(query, Integer.class, schema, tableName);
		return count != null && count > 0;
	}

	public void insertRow(String tableName, Map<String, Object> rowData) throws SQLException {

		if (rowData == null || rowData.isEmpty()) {
			throw new IllegalArgumentException("No dynamic data provided for insert.");
		}

		Set<String> insertColumns = rowData.keySet();
		validateInsertColumns(tableName, insertColumns, jdbcTemplate);

		// Normalize and convert data types
		Map<String, Object> insertData = normalizeInsertData(tableName, rowData, insertColumns);

		String columns = insertData.keySet().stream().map(col -> "`" + col + "`").collect(Collectors.joining(", "));

		String placeholders = insertData.keySet().stream().map(col -> "?").collect(Collectors.joining(", "));

		String sql = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + placeholders + ")";
		System.out.println("SQL: " + sql);
		System.out.println("Values: " + Arrays.toString(insertData.values().toArray()));
		System.out.println("Column count: " + insertData.keySet().size());

		jdbcTemplate.update(sql, insertData.values().toArray());
	}

	public void validateInsertColumns(String tableName, Set<String> insertColumns, JdbcTemplate jdbcTemplate) {
		// 1. Get actual columns from the database table
		Set<String> dbColumns = jdbcTemplate.query("SELECT * FROM `" + tableName + "` LIMIT 1", rs -> {
			ResultSetMetaData meta = rs.getMetaData();
			Set<String> columns = new HashSet<>();
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				columns.add(meta.getColumnName(i).toLowerCase());
			}
			return columns;
		});

		// 2. Filter out system-managed or backend-only columns
		Set<String> excluded = Set.of("created_date", "updated_date");
		dbColumns.removeAll(excluded);

		// 3. Normalize insert columns
		Set<String> cleanedColumns = insertColumns.stream().map(col -> col == null ? "" : col.trim().toLowerCase())
				.collect(Collectors.toSet());

		// 4. Check if any column in the insert list is not in the DB table
		Set<String> missingColumns = new HashSet<>(cleanedColumns);
		missingColumns.removeAll(dbColumns);

		if (!missingColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"Upload failed: Invalid column names found in your file: " + missingColumns);
		}
	}

	public Map<String, Object> normalizeInsertData(String tableName, Map<String, Object> rawData,
			Set<String> numericColumns) throws SQLException {

		Map<String, Integer> columnTypeMap = getColumnTypes(tableName);
		return rawData.entrySet().stream().filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty())
				.collect(Collector.of(LinkedHashMap::new, (map, e) -> {
					String col = e.getKey().trim();
					Object val = e.getValue();
					Integer sqlType = columnTypeMap.get(col.toLowerCase());

					Object finalValue;
					String valStr = (val != null) ? val.toString().trim() : null;

					if (sqlType == null) {
						finalValue = (valStr == null) ? "" : valStr;
					} else if (valStr == null || valStr.isEmpty()) {
						finalValue = (sqlType == Types.INTEGER || sqlType == Types.DECIMAL || sqlType == Types.NUMERIC
								|| sqlType == Types.DOUBLE || sqlType == Types.FLOAT || sqlType == Types.DATE
								|| sqlType == Types.TIMESTAMP) ? null : "";
					} else {
						try {
							if (val instanceof Boolean && (sqlType == Types.INTEGER || sqlType == Types.TINYINT
									|| sqlType == Types.SMALLINT || sqlType == Types.BIT)) {
								finalValue = (Boolean) val ? 1 : 0;
							} else {
								finalValue = switch (sqlType) {
								case Types.INTEGER -> Integer.parseInt(valStr);
								case Types.DECIMAL, Types.NUMERIC -> new BigDecimal(valStr);
								case Types.DOUBLE, Types.FLOAT -> Double.parseDouble(valStr);
								case Types.DATE -> Date.valueOf(valStr);
								case Types.TIMESTAMP -> Timestamp.valueOf(valStr);
								default -> valStr;
								};
							}
						} catch (Exception ex) {
							finalValue = valStr; // fallback
						}
					}

					map.put(col, finalValue);
				}, (map1, map2) -> {
					map1.putAll(map2);
					return map1;
				}));

	}

	public Map<String, Integer> getColumnTypes(String tableName) throws SQLException {
		Map<String, Integer> columnTypes = new HashMap<>();

		try (Connection connection = jdbcTemplate.getDataSource().getConnection();
				PreparedStatement stmt = connection.prepareStatement("SELECT * FROM `" + tableName + "` LIMIT 1");
				ResultSet rs = stmt.executeQuery()) {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String columnName = metaData.getColumnName(i).toLowerCase(); // normalize
				int columnType = metaData.getColumnType(i); // java.sql.Types
				columnTypes.put(columnName, columnType);
			}
		}

		return columnTypes;
	}
	
	public void createArchiveTable(String domainName, String tableName) throws SQLException {
	   
	    if (domainName.isEmpty()) {
	        throw new IllegalArgumentException("Domain name cannot be empty after sanitization.");
	    }

	    String idRefCol  = domainName + "_id"; // as requested
	    logger.info("Archive Domain id column", idRefCol);

	    String sql =
	        "CREATE TABLE IF NOT EXISTS `" + tableName + "` ("
	      +   " `id` VARCHAR(36) PRIMARY KEY,"   
	      + "  `" + idRefCol + "` VARCHAR(191) NOT NULL,"
	      + "  `message` TEXT NULL,"
	      + "  `archived_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
	      + ")";

	    // Optional: log for visibility
	    logger.info("Archived SQL: {}", sql);

	    jdbcTemplate.execute(sql);
	}

	private static String sanitizeIdentifier(String raw) {
	    if (raw == null) return "";
	    String s = raw.trim().toLowerCase();
	    s = s.replaceAll("[^a-z0-9_]", "_");
	    s = s.replaceAll("_+", "_");
	    s = s.replaceAll("^_+", "").replaceAll("_+$", "");
	    return s;
	}
	
	@Transactional(rollbackFor = Exception.class)
	public void insertArchiveData(String domainName, String idValue, String message) throws SQLException {
	    if (domainName == null || domainName.trim().isEmpty()) {
	        throw new IllegalArgumentException("Table name cannot be null or empty.");
	    }
	    
	    String schema = jdbcTemplate.getDataSource().getConnection().getCatalog();
	    String dn = sanitizeIdentifier(domainName);
	    String archiveTable = dn + "_archive";
		if (!tableExists(schema, archiveTable)) {
			createArchiveTable(dn, archiveTable);
		}
	    if (idValue == null) {
	        throw new IllegalArgumentException("ID value cannot be null.");
	    }

	    String idRefCol  = dn + "_id";
	    String primaryID = UUID.randomUUID().toString();
	    
	    // Insert into archive table
	    String insertSql = "INSERT INTO `" + archiveTable + "` (`id`, `" + idRefCol + "`, `message`, `archived_at`) "
	            + "VALUES (?, ?, ?, NOW())";

	    logger.info("Inserted archived SQL row: {}", insertSql);
	    jdbcTemplate.update(insertSql, primaryID, idValue, message);
	    
	    String updateSql = "UPDATE `" + dn + "` SET `is_archived` = ? WHERE `id` = ?";
	    logger.info("Updated archived status: {}", updateSql);
	    jdbcTemplate.update(updateSql, true, idValue);
	}
	
	public void updateColumnValue(String tableName, String columnName, String idValue, Object fromValue, Object toValue)
			throws SQLException {
		if (tableName == null || tableName.isBlank()) {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}
		if (columnName == null || columnName.isBlank()) {
			throw new IllegalArgumentException("Column name cannot be null or empty.");
		}
		if (idValue == null || idValue.isBlank()) {
			throw new IllegalArgumentException("ID value cannot be null or empty.");
		}
		if (fromValue == null) {
			throw new IllegalArgumentException("From-value cannot be null.");
		}


		String sql = "UPDATE " + tableName + " SET " + columnName + " = ? " + "WHERE id = ? AND " + columnName + " = ?";


		jdbcTemplate.update(sql, toValue, idValue, fromValue);
	}
}

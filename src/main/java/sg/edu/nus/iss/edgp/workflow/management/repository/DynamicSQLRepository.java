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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import sg.edu.nus.iss.edgp.workflow.management.utility.GeneralUtility;

@Repository
public class DynamicSQLRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

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

	public List<Map<String, Object>> findAllDomainDataList(String tableName, String userOrgId, String fileId) {
		if (tableName == null || tableName.isBlank()) {
			throw new IllegalArgumentException("Table name is required");
		}

		StringBuilder sql = new StringBuilder("SELECT * FROM ").append(backtick(tableName)).append(" WHERE ")
				.append(backtick("organization_id")).append(" = ?");

		List<Object> args = new ArrayList<>();
		args.add(userOrgId);

		if (GeneralUtility.hasText(fileId)) {
			sql.append(" AND ").append(backtick("file_id")).append(" = ?");
			args.add(fileId);
		}

		return jdbcTemplate.queryForList(sql.toString(), args.toArray());
	}

	public Page<Map<String, Object>> findPaginatedDomainDataList(String tableName, String userOrgId, String fileId,
			Pageable pageable) {

		if (tableName == null || tableName.isBlank()) {
			throw new IllegalArgumentException("Table name is required");
		}

		StringBuilder where = new StringBuilder().append(" WHERE ").append(backtick("organization_id")).append(" = ?");
		List<Object> filterArgs = new ArrayList<>();
		filterArgs.add(userOrgId);

		if (GeneralUtility.hasText(fileId)) {
			where.append(" AND ").append(backtick("file_id")).append(" = ?");
			filterArgs.add(fileId);
		}

		StringBuilder dataSql = new StringBuilder("SELECT * FROM ").append(backtick(tableName)).append(where);

		Set<String> ALLOWED_SORT_COLUMNS = Set.of("id", "created_date", "updated_date");
		if (pageable.getSort().isSorted()) {
			String orderBy = pageable.getSort().stream().filter(o -> ALLOWED_SORT_COLUMNS.contains(o.getProperty()))
					.map(o -> backtick(o.getProperty()) + (o.isAscending() ? " ASC" : " DESC"))
					.collect(java.util.stream.Collectors.joining(", "));
			if (!orderBy.isEmpty())
				dataSql.append(" ORDER BY ").append(orderBy);
		} else {
			dataSql.append(" ORDER BY ").append(backtick("id"));
		}
		dataSql.append(" LIMIT ? OFFSET ?");

		List<Object> dataArgs = new ArrayList<>(filterArgs);
		dataArgs.add(pageable.getPageSize());
		dataArgs.add(pageable.getOffset());

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(dataSql.toString(), dataArgs.toArray());

		String countSql = "SELECT COUNT(*) FROM " + backtick(tableName) + where.toString();
		long total = jdbcTemplate.queryForObject(countSql, Long.class, filterArgs.toArray());

		return new PageImpl<>(rows, pageable, total);
	}

	private String backtick(String ident) {
		return "`" + ident.replace("`", "``") + "`";
	}

}

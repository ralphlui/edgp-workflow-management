package sg.edu.nus.iss.edgp.workflow.management.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import sg.edu.nus.iss.edgp.workflow.management.utility.GeneralUtility;

@Repository
public class DomainDataRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	private static final Logger logger = LoggerFactory.getLogger(DomainDataRepository.class);

	public List<Map<String, Object>> findAllDomainDataList(String tableName, String userOrgId, String fileId, Boolean includeArchive) {
		if (tableName == null || tableName.isBlank()) {
			throw new IllegalArgumentException("Table name is required");
		}

		tableName = tableName.toLowerCase();
		StringBuilder sql = new StringBuilder();
		if (includeArchive == null || includeArchive) {
			 sql = new StringBuilder("SELECT * FROM ").append(backtick(tableName)).append(" WHERE ")
					.append(backtick("organization_id")).append(" = ?");
		} else {
			 sql = new StringBuilder("SELECT * FROM ")
					    .append(backtick(tableName))
					    .append(" WHERE ")
					    .append(backtick("organization_id")).append(" = ?")
					    .append(" AND ")
					    .append(backtick("is_archived")).append(" = false");
		}
		

		List<Object> args = new ArrayList<>();
		args.add(userOrgId);

		if (GeneralUtility.hasText(fileId)) {
			sql.append(" AND ").append(backtick("file_id")).append(" = ?");
			args.add(fileId);
		}

		logger.info("retrieving all domain data list.");
		return jdbcTemplate.queryForList(sql.toString(), args.toArray());
	}

	public Page<Map<String, Object>> findPaginatedDomainDataList(String tableName, String userOrgId, String fileId,
			Pageable pageable) {

		if (tableName == null || tableName.isBlank()) {
			throw new IllegalArgumentException("Table name is required");
		}

		tableName = tableName.toLowerCase();
		StringBuilder where = new StringBuilder().append(" WHERE ").append(backtick("organization_id")).append(" = ?");
		List<Object> filterArgs = new ArrayList<>();
		filterArgs.add(userOrgId);

		if (GeneralUtility.hasText(fileId)) {
			where.append(" AND ").append(backtick("file_id")).append(" = ?");
			filterArgs.add(fileId);
		}

		StringBuilder dataSql = new StringBuilder("SELECT * FROM ").append(backtick(tableName)).append(where);
		logger.info("created sql query while retrieving paginated domain data list.");

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

		logger.info("retrieving paginated domain data list.");
		return new PageImpl<>(rows, pageable, total);
	}

	public Map<String, Object> retrieveDetailDomainDataRecordById(String tableName, String id) {

		tableName = tableName.toLowerCase();

		String sql = "SELECT * FROM " + backtick(tableName) + " WHERE " + backtick("id") + " = ? LIMIT 1";

		logger.info("Retrieving detail domain data by id. table='{}', id='{}'", tableName, id);
		return jdbcTemplate.queryForMap(sql, id); // <-- pass the parameter
	}

	private String backtick(String ident) {
		return "`" + ident.replace("`", "``") + "`";
	}

}

package sg.edu.nus.iss.edgp.workflow.management.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

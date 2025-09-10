package sg.edu.nus.iss.edgp.workflow.management.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

@ExtendWith(MockitoExtension.class)
class DynamicSQLRepositoryTest {

	@Mock
	private JdbcTemplate jdbcTemplate;

	@Spy
	@InjectMocks
	private DynamicSQLRepository repo;

	@Test
	void tableExists_returnsTrueWhenCountPositive() {
		when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("myschema"), eq("mytable"))).thenReturn(1);

		boolean exists = repo.tableExists("myschema", "mytable");
		assertTrue(exists);
	}

	@Test
	void tableExists_returnsFalseWhenZero() {
		when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any())).thenReturn(0);

		assertFalse(repo.tableExists("sch", "tbl"));
	}

	@Test
	void tableExists_returnsFalseWhenNull() {
		when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any())).thenReturn(null);

		assertFalse(repo.tableExists("sch", "tbl"));
	}

	@Test
	void insertRow_throwsOnNullOrEmpty() {
		assertThrows(IllegalArgumentException.class, () -> repo.insertRow("users", null));
		assertThrows(IllegalArgumentException.class, () -> repo.insertRow("users", Map.of()));
	}

	@Test
	void insertRow_buildsSqlAndBindsNormalizedValues() throws Exception {

		Map<String, Object> raw = new LinkedHashMap<>();
		raw.put(" name ", " Alice ");
		raw.put("age", " 23 ");
		raw.put("is_active", true);

		doNothing().when(repo).validateInsertColumns(eq("users"), anySet(), eq(jdbcTemplate));

		Map<String, Integer> types = Map.of("name", Types.VARCHAR, "age", Types.INTEGER, "is_active", Types.TINYINT);
		doReturn(types).when(repo).getColumnTypes("users");

		ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<Object[]> argsCap = ArgumentCaptor.forClass(Object[].class);

		when(jdbcTemplate.update(sqlCap.capture(), argsCap.capture())).thenReturn(1);

		repo.insertRow("users", raw);

		String sql = sqlCap.getValue();
		assertEquals("INSERT INTO `users` (`name`, `age`, `is_active`) VALUES (?, ?, ?)", sql);

		Object[] args = argsCap.getValue();
		assertArrayEquals(new Object[] { "Alice", 23, 1 }, args);
	}

	@Test
	void validateInsertColumns_allowsValidTrimmedAndIgnoresExcluded() {

		mockJdbcQueryMetaColumnsReturn(Set.of("id", "name", "created_date"));

		Set<String> insertCols = Set.of("  NAME  ");
		assertDoesNotThrow(() -> repo.validateInsertColumns("customers", insertCols, jdbcTemplate));
	}

	@Test
	void validateInsertColumns_throwsOnMissingColumns() {
		mockJdbcQueryMetaColumnsReturn(Set.of("id", "name", "email"));

		Set<String> insertCols = Set.of("name", "ghost_field");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> repo.validateInsertColumns("customers", insertCols, jdbcTemplate));
		assertTrue(ex.getMessage().contains("ghost_field"));
	}

	@Test
	void normalizeInsertData_coercesTypesAndNulls() throws Exception {

		Map<String, Integer> types = new HashMap<>();
		types.put("age", Types.INTEGER);
		types.put("salary", Types.DECIMAL);
		types.put("join_date", Types.DATE);
		types.put("last_seen", Types.TIMESTAMP);
		types.put("note", Types.VARCHAR);
		types.put("active", Types.TINYINT);

		doReturn(types).when(repo).getColumnTypes("employees");

		Map<String, Object> raw = new LinkedHashMap<>();
		raw.put("age", " 45 ");
		raw.put("salary", " 1234.50 ");
		raw.put("join_date", "2025-08-01");
		raw.put("last_seen", "2025-08-01 10:20:30");
		raw.put("note", "  hello ");
		raw.put("active", false);
		raw.put("empty_for_numeric", "");

		Map<String, Object> out = repo.normalizeInsertData("employees", raw, raw.keySet());

		assertEquals(45, out.get("age"));
		assertEquals(new BigDecimal("1234.50"), out.get("salary"));
		assertEquals(Date.valueOf("2025-08-01"), out.get("join_date"));
		assertEquals(Timestamp.valueOf("2025-08-01 10:20:30"), out.get("last_seen"));
		assertEquals("hello", out.get("note"));
		assertEquals(0, out.get("active"));
		assertEquals("", out.get("empty_for_numeric"));
	}

	@Test
	void normalizeInsertData_setsNullForEmptyNumericAndDates() throws Exception {
		Map<String, Integer> types = Map.of("qty", Types.INTEGER, "amount", Types.NUMERIC, "when_at", Types.TIMESTAMP);
		doReturn(types).when(repo).getColumnTypes("orders");

		Map<String, Object> raw = new LinkedHashMap<>();
		raw.put("qty", "");
		raw.put("amount", "   ");
		raw.put("when_at", "");

		Map<String, Object> out = repo.normalizeInsertData("orders", raw, raw.keySet());
		assertNull(out.get("qty"));
		assertNull(out.get("amount"));
		assertNull(out.get("when_at"));
	}

	@Test
	void getColumnTypes_readsFromMetadata() throws Exception {
		DataSource ds = mock(DataSource.class);
		Connection conn = mock(Connection.class);
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		ResultSetMetaData meta = mock(ResultSetMetaData.class);

		when(jdbcTemplate.getDataSource()).thenReturn(ds);
		when(ds.getConnection()).thenReturn(conn);
		when(conn.prepareStatement("SELECT * FROM `products` LIMIT 1")).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.getMetaData()).thenReturn(meta);

		when(meta.getColumnCount()).thenReturn(3);
		when(meta.getColumnName(1)).thenReturn("ID");
		when(meta.getColumnType(1)).thenReturn(Types.INTEGER);
		when(meta.getColumnName(2)).thenReturn("NAME");
		when(meta.getColumnType(2)).thenReturn(Types.VARCHAR);
		when(meta.getColumnName(3)).thenReturn("PRICE");
		when(meta.getColumnType(3)).thenReturn(Types.DECIMAL);

		Map<String, Integer> colTypes = repo.getColumnTypes("products");
		assertEquals(3, colTypes.size());
		assertEquals(Types.INTEGER, colTypes.get("id"));
		assertEquals(Types.VARCHAR, colTypes.get("name"));
		assertEquals(Types.DECIMAL, colTypes.get("price"));

		verify(ps).executeQuery();
		verify(conn).prepareStatement("SELECT * FROM `products` LIMIT 1");
	}

	@SuppressWarnings("unchecked")
	private void mockJdbcQueryMetaColumnsReturn(Set<String> columnsLowercase) {
		when(jdbcTemplate.query(startsWith("SELECT * FROM `"), any(ResultSetExtractor.class))).thenAnswer(inv -> {
			ResultSetExtractor<Set<String>> extractor = inv.getArgument(1);
			ResultSet rs = mock(ResultSet.class);
			ResultSetMetaData meta = mock(ResultSetMetaData.class);

			List<String> cols = new ArrayList<>(columnsLowercase).stream().map(String::toUpperCase)
					.collect(Collectors.toList());
			when(rs.getMetaData()).thenReturn(meta);
			when(meta.getColumnCount()).thenReturn(cols.size());
			for (int i = 0; i < cols.size(); i++) {
				when(meta.getColumnName(i + 1)).thenReturn(cols.get(i));
			}

			return extractor.extractData(rs);
		});
	}
}

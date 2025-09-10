package sg.edu.nus.iss.edgp.workflow.management.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicSQLServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DynamicSQLRepository;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicSQLService;

@ExtendWith(MockitoExtension.class)
class DynamicSQLServiceTest {

	@Mock
	private JdbcTemplate jdbcTemplate;
	@Mock
	private DataSource dataSource;
	@Mock
	private Connection connection;
	@Mock
	private DynamicSQLRepository dynamicSQLRepository;

	private DynamicSQLService service;

	@BeforeEach
	void setUp() {
		service = new DynamicSQLService(dynamicSQLRepository);
		ReflectionTestUtils.setField(service, "jdbcTemplate", jdbcTemplate);
	}

	private void stubSchemaCatalog(String catalog) throws SQLException {
		when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getCatalog()).thenReturn(catalog);
	}

	@Test
	void buildCreateTableSQL_happyPath_executesCreateAndInserts() throws SQLException {

		Map<String, Object> data = new HashMap<>();
		data.put("Name", "Alice");
		data.put("Age", 30);
		data.put("Active", true);

		// This path calls insertData → we need schema
		stubSchemaCatalog("test_schema");
		when(dynamicSQLRepository.tableExists("test_schema", "users")).thenReturn(true);

		// Spy to capture insertData call while keeping existing injections
		DynamicSQLService spyService = spy(service);
		ReflectionTestUtils.setField(spyService, "jdbcTemplate", jdbcTemplate);

		// Act
		spyService.buildCreateTableSQL(data, "Users");

		// Assert: CREATE TABLE executed
		ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
		verify(jdbcTemplate).execute(sqlCap.capture());
		String sql = sqlCap.getValue();

		assertTrue(sql.startsWith("CREATE TABLE IF NOT EXISTS `users` ("));
		assertTrue(sql.contains("`id` VARCHAR(36) PRIMARY KEY"));
		assertTrue(sql.contains("`created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP"));
		assertTrue(sql.contains("`updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"));
		assertTrue(sql.contains("`name` VARCHAR(255)"));
		assertTrue(sql.contains("`age` INT"));
		assertTrue(sql.contains("`active` BOOLEAN"));

		ArgumentCaptor<String> tableCap = ArgumentCaptor.forClass(String.class);
		@SuppressWarnings("unchecked")
		ArgumentCaptor<Map<String, Object>> mapCap = ArgumentCaptor.forClass(Map.class);
		verify(spyService).insertData(tableCap.capture(), mapCap.capture());

		assertEquals("users", tableCap.getValue());
		assertSame(data, mapCap.getValue());
		assertTrue(data.containsKey("id"));
		assertTrue(data.get("id") instanceof String);
	}

	@Test
	void buildCreateTableSQL_emptyTable_throwsServiceException() {
		Map<String, Object> data = Map.of("x", 1);

		DynamicSQLServiceException ex = assertThrows(DynamicSQLServiceException.class,
				() -> service.buildCreateTableSQL(data, ""));

		assertTrue(ex.getMessage().contains("creating clean data table"));

		assertNotNull(ex.getCause());
		assertTrue(ex.getCause() instanceof DynamicSQLServiceException);
		assertTrue(ex.getCause().getMessage().contains("Table Name is empty"));

		verify(jdbcTemplate, never()).execute(anyString());
		verifyNoInteractions(dynamicSQLRepository);
	}

	@Test
	void buildCreateTableSQL_executeFails_wrapsInServiceException() {

		Map<String, Object> data = Map.of("x", 1);
		doThrow(new RuntimeException("DDL fail")).when(jdbcTemplate).execute(anyString());

		DynamicSQLServiceException ex = assertThrows(DynamicSQLServiceException.class,
				() -> service.buildCreateTableSQL(data, "tbl"));

		assertTrue(ex.getMessage().contains("creating clean data table"));
		verify(jdbcTemplate).execute(anyString());

	}

	@Test
	void insertData_happyPath_addsIdAndCallsInsertRow() throws SQLException {
		Map<String, Object> data = new HashMap<>();
		data.put("col1", "v1");

		stubSchemaCatalog("test_schema");
		when(dynamicSQLRepository.tableExists("test_schema", "orders")).thenReturn(true);

		service.insertData("Orders", data);

		verify(dynamicSQLRepository).tableExists("test_schema", "orders");

		@SuppressWarnings("unchecked")
		ArgumentCaptor<Map<String, Object>> mapCap = ArgumentCaptor.forClass(Map.class);
		verify(dynamicSQLRepository).insertRow(eq("orders"), mapCap.capture());

		Map<String, Object> saved = mapCap.getValue();
		assertEquals("v1", saved.get("col1"));
		assertNotNull(saved.get("id"));
		assertTrue(saved.get("id") instanceof String);
	}

	@Test
	void insertData_tableNotExists_throwsServiceException() throws SQLException {
		Map<String, Object> data = new HashMap<>();

		stubSchemaCatalog("test_schema");
		when(dynamicSQLRepository.tableExists("test_schema", "clean_table")).thenReturn(false);

		DynamicSQLServiceException ex = assertThrows(DynamicSQLServiceException.class,
				() -> service.insertData("CLEAN_TABLE", data));

		assertTrue(ex.getMessage().contains("inserting clean data"));
		verify(dynamicSQLRepository).tableExists("test_schema", "clean_table");
		verify(dynamicSQLRepository, never()).insertRow(anyString(), anyMap());
	}

	@Test
	void insertData_repositoryThrows_wrapsInServiceException() throws SQLException {
		Map<String, Object> data = new HashMap<>();

		stubSchemaCatalog("test_schema");
		when(dynamicSQLRepository.tableExists("test_schema", "t1")).thenReturn(true);
		doThrow(new RuntimeException("DB down")).when(dynamicSQLRepository).insertRow(anyString(), anyMap());

		DynamicSQLServiceException ex = assertThrows(DynamicSQLServiceException.class,
				() -> service.insertData("T1", data));

		assertTrue(ex.getMessage().contains("inserting clean data"));
		verify(dynamicSQLRepository).insertRow(eq("t1"), anyMap());
	}
}

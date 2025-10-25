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

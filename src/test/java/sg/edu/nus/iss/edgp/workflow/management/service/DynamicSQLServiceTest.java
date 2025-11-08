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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
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
	
	@Test
	void buildCreateTableSQL_happyPath_createsAltersAndInserts() throws Exception {
	   
	    String table = "Customer_Events";
	    Map<String, Object> data = new HashMap<>();
	    data.put("Customer Id", 123L); 
	    data.put("Amount", new java.math.BigDecimal("12.34")); 
	    data.put("active", true);  
	    data.put("note", "hello"); 

	    
	    ArgumentCaptor<String> sqlExecCaptor = ArgumentCaptor.forClass(String.class);
	    doNothing().when(jdbcTemplate).execute(sqlExecCaptor.capture());
 
	    when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
	    when(dataSource.getConnection()).thenReturn(connection);
	    when(connection.getCatalog()).thenReturn("db1");

	    DatabaseMetaData meta = mock(DatabaseMetaData.class);
	    when(connection.getMetaData()).thenReturn(meta);

	    ResultSet rs = mock(ResultSet.class);
	    when(meta.getColumns(eq("db1"), isNull(), eq(table.toLowerCase()), isNull())).thenReturn(rs);

	     
	    when(rs.next()).thenReturn(true, true, true, true, false);
	    when(rs.getString("COLUMN_NAME"))
	            .thenReturn("id", "created_date", "updated_date", "is_archived");

	    
	    when(dynamicSQLRepository.tableExists("db1", table.toLowerCase())).thenReturn(true);
	    doNothing().when(dynamicSQLRepository).insertRow(anyString(), anyMap());

	  
	    service.buildCreateTableSQL(data, table);

	  
	    var executedSqls = sqlExecCaptor.getAllValues();
	    assertFalse(executedSqls.isEmpty());

	     
	    assertTrue(
	            executedSqls.stream().anyMatch(sql ->
	                    sql.startsWith("CREATE TABLE IF NOT EXISTS `customer_events` (")
	                            && sql.contains("`id` VARCHAR(36) PRIMARY KEY")
	                            && sql.contains("`created_date` TIMESTAMP")
	                            && sql.contains("`updated_date` TIMESTAMP")
	                            && sql.contains("`is_archived` BOOLEAN")
	            ),
	            "Should create table with static columns"
	    );

	 
	    assertTrue(
	            executedSqls.stream().anyMatch(sql ->
	                    sql.equals("ALTER TABLE `customer_events` ADD COLUMN `customer_id` BIGINT")),
	            "Missing ALTER for customer_id BIGINT"
	    );
	    assertTrue(
	            executedSqls.stream().anyMatch(sql ->
	                    sql.equals("ALTER TABLE `customer_events` ADD COLUMN `amount` DECIMAL(38,10)")),
	            "Missing ALTER for amount DECIMAL(38,10)"
	    );
	    assertTrue(
	            executedSqls.stream().anyMatch(sql ->
	                    sql.equals("ALTER TABLE `customer_events` ADD COLUMN `active` BOOLEAN")),
	            "Missing ALTER for active BOOLEAN"
	    );
	    assertTrue(
	            executedSqls.stream().anyMatch(sql ->
	                    sql.equals("ALTER TABLE `customer_events` ADD COLUMN `note` VARCHAR(255)")),
	            "Missing ALTER for note VARCHAR(255)"
	    );

	    
	    ArgumentCaptor<Map<String, Object>> rowCaptor = ArgumentCaptor.forClass(Map.class);
	    verify(dynamicSQLRepository).insertRow(eq("customer_events"), rowCaptor.capture());
	    Map<String, Object> inserted = rowCaptor.getValue();
	    
	    assertTrue(inserted.containsKey("id"));
	    assertEquals(123L, inserted.get("customer_id"));
	    assertEquals(new java.math.BigDecimal("12.34"), inserted.get("amount"));
	    assertEquals(true, inserted.get("active"));
	    assertEquals("hello", inserted.get("note"));
	}

	@Test
	void buildCreateTableSQL_typeMappingAndNormalization_variants() throws Exception {
	   
	    Map<String, Object> data = new HashMap<>();
	    data.put("INT_col", 5);
	    data.put("DOUBLE_col", 1.23d);
	    data.put("FLOAT_col", 2.34f);
	    data.put("DEC_col", new java.math.BigDecimal("9.99"));
	    data.put("BOOL_col", Boolean.FALSE);
	    data.put("DATE_col", java.sql.Date.valueOf("2025-01-01"));
	    data.put("LDATE_col", java.time.LocalDate.of(2025,1,2));
	    data.put("TS_col", java.sql.Timestamp.valueOf("2025-01-01 10:20:30"));
	    data.put("LDT_col", java.time.LocalDateTime.of(2025,1,2,3,4,5));
	    data.put("STR_col", "abc");
	    data.put("null_col", null);
	   
	    data.put("Mixed Name", "x");

	    
	    ArgumentCaptor<String> sqlExecCaptor = ArgumentCaptor.forClass(String.class);
	    doNothing().when(jdbcTemplate).execute(sqlExecCaptor.capture());

	   
	    when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
	    when(dataSource.getConnection()).thenReturn(connection);
	    when(connection.getCatalog()).thenReturn("dbx");

	    DatabaseMetaData meta = mock(DatabaseMetaData.class);
	    when(connection.getMetaData()).thenReturn(meta);
	    ResultSet rs = mock(ResultSet.class);
	    when(meta.getColumns(eq("dbx"), isNull(), eq("events"), isNull())).thenReturn(rs);
	    when(rs.next()).thenReturn(true, true, true, true, false);
	    when(rs.getString("COLUMN_NAME"))
	            .thenReturn("id", "created_date", "updated_date", "is_archived");

	    when(dynamicSQLRepository.tableExists("dbx", "events")).thenReturn(true);
	    doNothing().when(dynamicSQLRepository).insertRow(anyString(), anyMap());

	    // Act
	    service.buildCreateTableSQL(data, "EVENTS");
 
	    var sqls = sqlExecCaptor.getAllValues();
 
	    java.util.function.BiConsumer<String, String> assertAlter = (col, type) -> assertTrue(
	            sqls.stream().anyMatch(s -> s.equals("ALTER TABLE `events` ADD COLUMN `" + col + "` " + type)),
	            () -> "Missing ALTER for `" + col + "` " + type + " in\n" + String.join("\n", sqls)
	    );

	    assertAlter.accept("int_col", "BIGINT");
	    assertAlter.accept("double_col", "DECIMAL(38,10)");
	    assertAlter.accept("float_col", "DECIMAL(38,10)");
	    assertAlter.accept("dec_col", "DECIMAL(38,10)");
	    assertAlter.accept("bool_col", "BOOLEAN");
	    assertAlter.accept("date_col", "DATE");
	    assertAlter.accept("ldate_col", "DATE");
	    assertAlter.accept("ts_col", "TIMESTAMP");
	    assertAlter.accept("ldt_col", "TIMESTAMP");
	    assertAlter.accept("str_col", "VARCHAR(255)");
	    assertAlter.accept("null_col", "VARCHAR(255)");
	    assertAlter.accept("mixed_name", "VARCHAR(255)");
 
	    verify(dynamicSQLRepository).insertRow(eq("events"), anyMap());
	}

}

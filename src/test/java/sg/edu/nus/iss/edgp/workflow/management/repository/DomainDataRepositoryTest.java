package sg.edu.nus.iss.edgp.workflow.management.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.aryEq;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;
import org.springframework.jdbc.core.JdbcTemplate;

@ExtendWith(MockitoExtension.class)
class DomainDataRepositoryTest {

	@Mock
	private JdbcTemplate jdbcTemplate;

	@InjectMocks
	private DomainDataRepository repo;

	@Test
	void findAllDomainDataList_throwsWhenTableBlank() {
		assertThrows(IllegalArgumentException.class, () -> repo.findAllDomainDataList("  ", "org-1", null, Boolean.TRUE));
		assertThrows(IllegalArgumentException.class, () -> repo.findAllDomainDataList(null, "org-1", "f1", Boolean.TRUE));
	}

	
	@Test
	void findAllDomainDataList_withoutFileId_buildsSqlAndArgs1() {
	    String table = "MyTable";
	    String expectedSql = "SELECT * FROM `mytable` WHERE `organization_id` = ?";

	    List<Map<String, Object>> rows = List.of(Map.of("id", "1"));
	    when(jdbcTemplate.queryForList(eq(expectedSql), aryEq(new Object[] { "ORG-9" }))).thenReturn(rows);

	    // Use TRUE (or null) instead of anyBoolean()
	    List<Map<String, Object>> out = repo.findAllDomainDataList(table, "ORG-9", null, Boolean.TRUE);

	    assertEquals(rows, out);
	    verify(jdbcTemplate).queryForList(eq(expectedSql), aryEq(new Object[] { "ORG-9" }));
	}

	@Test
	void findAllDomainDataList_withFileId_buildsSqlAndArgs() {
		String table = "data_tbl";
		String expectedSql = "SELECT * FROM `data_tbl` WHERE `organization_id` = ? AND `file_id` = ?";

		List<Map<String, Object>> rows = List.of(Map.of("id", "22"));
		when(jdbcTemplate.queryForList(eq(expectedSql), aryEq(new Object[] { "ORG-X", "F-77"}))).thenReturn(rows);

		List<Map<String, Object>> out = repo.findAllDomainDataList(table, "ORG-X", "F-77", Boolean.TRUE);

		assertEquals(rows, out);
		verify(jdbcTemplate).queryForList(eq(expectedSql), aryEq(new Object[] { "ORG-X", "F-77" }));
	}

	@Test
	void findPaginatedDomainDataList_unsorted_defaultsToOrderById() {
		String table = "Domain_A";
		Pageable pageable = PageRequest.of(2, 25); // page 2 -> offset 50

		String expectedSelect = "SELECT * FROM `domain_a` WHERE `organization_id` = ? ORDER BY `id` LIMIT ? OFFSET ?";
		String expectedCount = "SELECT COUNT(*) FROM `domain_a` WHERE `organization_id` = ?";

		List<Map<String, Object>> rows = List.of(Map.of("id", "a1"), Map.of("id", "a2"));
		when(jdbcTemplate.queryForList(eq(expectedSelect), aryEq(new Object[] { "ORG-1", 25, 50L }))).thenReturn(rows);
		when(jdbcTemplate.queryForObject(eq(expectedCount), eq(Long.class), aryEq(new Object[] { "ORG-1" })))
				.thenReturn(200L);

		Page<Map<String, Object>> page = repo.findPaginatedDomainDataList(table, "ORG-1", null, pageable);

		assertEquals(2, page.getContent().size());
		assertEquals(200, page.getTotalElements());
		assertEquals(pageable, page.getPageable());

		verify(jdbcTemplate).queryForList(eq(expectedSelect), aryEq(new Object[] { "ORG-1", 25, 50L }));
		verify(jdbcTemplate).queryForObject(eq(expectedCount), eq(Long.class), aryEq(new Object[] { "ORG-1" }));
	}

	@Test
	void findPaginatedDomainDataList_withFileId_andAllowedSorts_onlyAllowedApplied() {
		String table = "orders";

		Sort sort = Sort.by(Sort.Order.desc("created_date"), Sort.Order.asc("updated_date"),
				Sort.Order.desc("not_allowed"));
		Pageable pageable = PageRequest.of(0, 10, sort);

		String expectedSelect = "SELECT * FROM `orders` WHERE `organization_id` = ? AND `file_id` = ? "
				+ "ORDER BY `created_date` DESC, `updated_date` ASC LIMIT ? OFFSET ?";
		String expectedCount = "SELECT COUNT(*) FROM `orders` WHERE `organization_id` = ? AND `file_id` = ?";

		List<Map<String, Object>> rows = List.of(Map.of("id", "1"));
		when(jdbcTemplate.queryForList(eq(expectedSelect), aryEq(new Object[] { "ORG-5", "FILE-9", 10, 0L })))
				.thenReturn(rows);
		when(jdbcTemplate.queryForObject(eq(expectedCount), eq(Long.class), aryEq(new Object[] { "ORG-5", "FILE-9" })))
				.thenReturn(1L);

		Page<Map<String, Object>> page = repo.findPaginatedDomainDataList(table, "ORG-5", "FILE-9", pageable);

		assertEquals(1, page.getTotalElements());
		assertEquals(rows, page.getContent());
	}

	@Test
	void retrieveDetailDomainDataRecordById_generatesSqlAndBindsId() {
		String expectedSql = "SELECT * FROM `customer_info` WHERE `id` = ? LIMIT 1";
		Map<String, Object> row = Map.of("id", "42", "name", "Alice");

		when(jdbcTemplate.queryForMap(eq(expectedSql), eq("42"))).thenReturn(row);

		Map<String, Object> out = repo.retrieveDetailDomainDataRecordById("Customer_Info", "42");
		assertEquals(row, out);

		verify(jdbcTemplate).queryForMap(eq(expectedSql), eq("42"));
	}
}

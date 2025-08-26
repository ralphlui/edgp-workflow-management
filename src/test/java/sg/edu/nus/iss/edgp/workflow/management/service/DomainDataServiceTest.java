package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import sg.edu.nus.iss.edgp.workflow.management.exception.DomainDataServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DomainDataRepository;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DomainDataService;

@ExtendWith(MockitoExtension.class)
public class DomainDataServiceTest {

	@Mock
	private DomainDataRepository domainDataRepository;

	@InjectMocks
	private DomainDataService service;

	@Test
	void retrieveAllDomainDataList_nonEmpty() {
		String domain = "customer";
		String orgId = "org-123";
		String fileId = "file-abc";

		List<Map<String, Object>> repoData = new ArrayList<>();
		Map<String, Object> row1 = new HashMap<>();
		row1.put("id", 1L);
		row1.put("name", "Alice");
		Map<String, Object> row2 = new HashMap<>();
		row2.put("id", 2L);
		row2.put("name", "Bob");
		repoData.add(row1);
		repoData.add(row2);

		when(domainDataRepository.findAllDomainDataList(domain, orgId, fileId)).thenReturn(repoData);

		Map<Long, List<Map<String, Object>>> result = service.retrieveAllDomainDataList(domain, orgId, fileId);

		// verify repository called with exact args
		verify(domainDataRepository).findAllDomainDataList(domain, orgId, fileId);

		// assert result has exactly one entry keyed by total count
		assertEquals(1, result.size(), "result should contain exactly one entry");
		assertTrue(result.containsKey(2L), "key should be total record count (2)");

		List<Map<String, Object>> out = result.get(2L);
		assertNotNull(out);
		assertEquals(repoData, out, "list should be the same instance returned by repository");

	}

	@Test
	void retrieveAllDomainDataList_empty() {
		String domain = "orders";
		String orgId = "org-xyz";
		String fileId = "file-empty";

		when(domainDataRepository.findAllDomainDataList(domain, orgId, fileId)).thenReturn(Collections.emptyList());

		Map<Long, List<Map<String, Object>>> result = service.retrieveAllDomainDataList(domain, orgId, fileId);

		assertEquals(1, result.size());
		assertTrue(result.containsKey(0L));
		assertTrue(result.get(0L).isEmpty());

	}

	@Test
	void retrieveAllDomainDataList_exception() {
		String domain = "invoices";
		String orgId = "org-err";
		String fileId = "file-err";

		RuntimeException root = new RuntimeException("db down");
		when(domainDataRepository.findAllDomainDataList(domain, orgId, fileId)).thenThrow(root);

		DomainDataServiceException ex = assertThrows(DomainDataServiceException.class,
				() -> service.retrieveAllDomainDataList(domain, orgId, fileId));

		assertEquals("An error occurred while retrieving all data list", ex.getMessage());
		assertSame(root, ex.getCause());

	}

	@Test
	void retrievePaginatedDomainDataList_nonEmpty() {
		String domain = "customer";
		String orgId = "org-123";
		String fileId = "file-abc";
		Pageable pageable = PageRequest.of(1, 2, Sort.by("id").ascending());

		// page content contains 2 rows, but total elements across all pages is 5
		List<Map<String, Object>> content = new ArrayList<>();
		Map<String, Object> r1 = new HashMap<>();
		r1.put("id", 3L);
		r1.put("name", "Carol");
		Map<String, Object> r2 = new HashMap<>();
		r2.put("id", 4L);
		r2.put("name", "Dave");
		content.add(r1);
		content.add(r2);

		Page<Map<String, Object>> page = new PageImpl<>(content, pageable, 5);

		when(domainDataRepository.findPaginatedDomainDataList(domain, orgId, fileId, pageable)).thenReturn(page);

		Map<Long, List<Map<String, Object>>> result = service.retrievePaginatedDomainDataList(domain, orgId, fileId,
				pageable);

		// repository called with exact args
		verify(domainDataRepository).findPaginatedDomainDataList(domain, orgId, fileId, pageable);

		// one entry keyed by total elements (5), value is page.getContent()
		assertEquals(1, result.size());
		assertTrue(result.containsKey(5L), "key should equal total elements (5)");

	}

	@Test
	void retrievePaginatedDomainDataList_empty() {
		String domain = "orders";
		String orgId = "org-xyz";
		String fileId = "file-empty";
		Pageable pageable = PageRequest.of(0, 10);

		Page<Map<String, Object>> emptyPage = new PageImpl<>(Collections.emptyList(), pageable, 0);

		when(domainDataRepository.findPaginatedDomainDataList(domain, orgId, fileId, pageable)).thenReturn(emptyPage);

		Map<Long, List<Map<String, Object>>> result = service.retrievePaginatedDomainDataList(domain, orgId, fileId,
				pageable);

		assertEquals(1, result.size());
		assertTrue(result.containsKey(0L));
		assertTrue(result.get(0L).isEmpty());

	}

	@Test
	void retrievePaginatedDomainDataList_exception() {
		String domain = "invoices";
		String orgId = "org-err";
		String fileId = "file-err";
		Pageable pageable = PageRequest.of(0, 5);

		RuntimeException root = new RuntimeException("db down");
		when(domainDataRepository.findPaginatedDomainDataList(domain, orgId, fileId, pageable)).thenThrow(root);

		DomainDataServiceException ex = assertThrows(DomainDataServiceException.class,
				() -> service.retrievePaginatedDomainDataList(domain, orgId, fileId, pageable));

		assertEquals("An error occurred while retrieving all data list", ex.getMessage());
		assertSame(root, ex.getCause());

	}
}

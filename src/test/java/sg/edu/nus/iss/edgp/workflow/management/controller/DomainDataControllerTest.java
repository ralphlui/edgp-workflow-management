package sg.edu.nus.iss.edgp.workflow.management.controller;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.Pageable;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.*;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import sg.edu.nus.iss.edgp.workflow.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DomainDataService;
import sg.edu.nus.iss.edgp.workflow.management.strategy.impl.ValidationStrategy;
import sg.edu.nus.iss.edgp.workflow.management.exception.DomainDataServiceException;
import sg.edu.nus.iss.edgp.workflow.management.enums.HTTPVerb;

@WebMvcTest(DomainDataController.class)
@AutoConfigureMockMvc(addFilters = false)
public class DomainDataControllerTest {

	private static final String ENDPOINT = "/api/wfm/domainData";
	private static final String AUTH = "Bearer good.jwt.token";
	private static final String FILE_ID = "file-1";

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private JWTService jwtService;
	@MockitoBean
	private AuditService auditService;
	@MockitoBean
	private ValidationStrategy validationStrategy;
	@MockitoBean
	private DomainDataService domainDataService;

	@BeforeEach
	void setup() {
		when(jwtService.extractUserIdFromToken("good.jwt.token")).thenReturn("user-1");
		when(jwtService.extractOrgIdFromToken("good.jwt.token")).thenReturn("org-1");

		ValidationResult ok = mock(ValidationResult.class);
		when(ok.isValid()).thenReturn(true);
		when(ok.getStatus()).thenReturn(HttpStatus.OK);
		when(ok.getMessage()).thenReturn("OK");
		when(validationStrategy.validateDomainAndOrgAccess(eq("finance"), eq("org-1"), eq(AUTH))).thenReturn(ok);

		when(validationStrategy.validateDomainAndOrgAccess(eq("retail"), eq("org-1"), eq(AUTH))).thenReturn(ok);

		when(validationStrategy.validateDomainAndOrgAccess(eq("legal"), eq("org-1"), eq(AUTH))).thenReturn(ok);

		when(validationStrategy.isUserOrganizationValidAndActive(eq("org-1"), eq("org-1"), eq(AUTH))).thenReturn(ok);

		when(auditService.createAuditDTO(anyString(), anyString(), anyString(), anyString(), any(HTTPVerb.class)))
				.thenReturn(Mockito.mock(AuditDTO.class));
		doNothing().when(auditService).logAudit(any(AuditDTO.class), anyInt(), anyString(), anyString());
	}

	@Test
	void retrieveDomainDataList_success_all() throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Object> r1 = new HashMap<>();
		r1.put("id", 101);
		r1.put("name", "alpha");
		rows.add(r1);

		Map<Long, List<Map<String, Object>>> svcResult = new LinkedHashMap<>();
		svcResult.put(10L, rows);
		when(domainDataService.retrieveAllDomainDataList(eq("finance"), eq("org-1"), eq(FILE_ID)))
				.thenReturn(svcResult);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID)
				.param("domainName", "finance").param("size", "10")) // omit "page" to make it null
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.message").value("Successfully retrieved all domain data list."))
				.andExpect(jsonPath("$.data[0].id").value(101))
				.andExpect(content().string(Matchers.containsString("\"totalRecord\":10"))).andDo(print());

		verify(auditService).logAudit(any(AuditDTO.class), eq(200), eq("Successfully retrieved all domain data list."),
				eq(AUTH));
	}

	@Test
	void retrieveDomainDataList_success_paginated() throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Object> r1 = new HashMap<>();
		r1.put("id", 202);
		r1.put("name", "beta");
		rows.add(r1);

		Map<Long, List<Map<String, Object>>> svcResult = new LinkedHashMap<>();
		svcResult.put(42L, rows);

		when(domainDataService.retrievePaginatedDomainDataList(eq("retail"), eq("org-1"), eq(FILE_ID),
				any(Pageable.class))).thenReturn(svcResult);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID)
				.param("domainName", "retail").param("page", "2").param("size", "5")).andExpect(status().isOk())
				.andExpect(jsonPath("$.message").value("Successfully retrieved all domain data list."))
				.andExpect(jsonPath("$.data[0].id").value(202))
				.andExpect(content().string(Matchers.containsString("\"totalRecord\":42"))).andDo(print());
	}

	@Test
	void retrieveDomainDataList_empty() throws Exception {
		Map<Long, List<Map<String, Object>>> svcResult = new LinkedHashMap<>();
		svcResult.put(0L, Collections.emptyList());

		when(domainDataService.retrieveAllDomainDataList(eq("legal"), eq("org-1"), eq(FILE_ID))).thenReturn(svcResult);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID)
				.param("domainName", "legal").param("size", "10")).andExpect(status().isOk())
				.andExpect(jsonPath("$.message").value("No Domain Data  List.")).andExpect(jsonPath("$.data").isArray())
				.andExpect(jsonPath("$.data", Matchers.hasSize(0))).andDo(print());

		verify(auditService).logAudit(any(AuditDTO.class), eq(200), eq("No Domain Data  List."), eq(AUTH));
	}

	@Test
	void retrieveDomainDataList_validationFailure() throws Exception {
		ValidationResult invalid = mock(ValidationResult.class);
		when(invalid.isValid()).thenReturn(false);
		when(invalid.getStatus()).thenReturn(HttpStatus.FORBIDDEN);
		when(invalid.getMessage()).thenReturn("No access to domain");
		when(validationStrategy.validateDomainAndOrgAccess(eq("finance"), eq("org-1"), eq(AUTH))).thenReturn(invalid);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID)
				.param("domainName", "finance").param("size", "10")).andExpect(status().isForbidden())
				.andExpect(jsonPath("$.message").value("No access to domain")).andDo(print());

		verify(auditService).logAudit(any(AuditDTO.class), eq(400), eq("No access to domain"), eq(AUTH));
		verifyNoInteractions(domainDataService);
	}

	@Test
	void retrieveDomainDataList_serviceException() throws Exception {
		when(domainDataService.retrieveAllDomainDataList(eq("finance"), eq("org-1"), eq(FILE_ID)))
				.thenThrow(new DomainDataServiceException("Domain retrieval failed"));

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID)
				.param("domainName", "finance").param("size", "10")).andExpect(status().isInternalServerError())
				.andExpect(jsonPath("$.message").value("Domain retrieval failed")).andDo(print());

		verify(auditService).logAudit(any(AuditDTO.class), eq(500), eq("Domain retrieval failed"), eq(AUTH));
	}

	@Test
	void getDomainDataById_success() throws Exception {
		Map<String, Object> record = new HashMap<>();
		record.put("id", "123");
		record.put("organization_id", "org-1");
		record.put("name", "Alpha");

		when(domainDataService.retrieveDetailDomainDataRecordById("finance", "123")).thenReturn(record);

		mockMvc.perform(
				get(ENDPOINT+"/my-domain-data").header("Authorization", AUTH).header("X-Id", "123").param("domainName", "finance"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.message").value("Requested domain data record is available."))
				.andExpect(jsonPath("$.data.id").value("123"))
				.andExpect(jsonPath("$.data.organization_id").value("org-1"));

		verify(auditService).logAudit(any(AuditDTO.class), eq(200), eq("Requested domain data record is available."),
				eq(AUTH));
	}

	@Test
	void getDomainDataById_badRequest() throws Exception {
		// Blank id triggers the bad request branch
		mockMvc.perform(get(ENDPOINT+"/my-domain-data").header("Authorization", AUTH).header("X-Id", "").param("domainName", "finance"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.message").value("Bad Request: Id or domain could not be blank."));

		verify(auditService).logAudit(any(AuditDTO.class), eq(400), eq("Bad Request: Id or domain could not be blank."),
				eq(AUTH));
		verifyNoInteractions(domainDataService);
		verifyNoInteractions(validationStrategy);
	}

	@Test
	void getDomainDataById_unauthorized() throws Exception {
		Map<String, Object> record = new HashMap<>();
		record.put("id", "999");
		record.put("organization_id", "org-other");
		when(domainDataService.retrieveDetailDomainDataRecordById("retail", "999")).thenReturn(record);

		ValidationResult invalid = mock(ValidationResult.class);
		when(invalid.isValid()).thenReturn(false);
		when(invalid.getStatus()).thenReturn(HttpStatus.UNAUTHORIZED);
		when(invalid.getMessage()).thenReturn("Unauthorized");
		when(validationStrategy.isUserOrganizationValidAndActive(eq("org-other"), eq("org-1"), eq(AUTH)))
				.thenReturn(invalid);

		mockMvc.perform(get(ENDPOINT+"/my-domain-data").header("Authorization", AUTH).header("X-Id", "999").param("domainName", "retail"))
				.andExpect(status().isUnauthorized())
				.andExpect(jsonPath("$.message").value("Unauthorized to view this domain data record."));

		verify(auditService).logAudit(any(AuditDTO.class), eq(401), eq("Unauthorized to view this domain data record."),
				eq(AUTH));
	}

	@Test
	void getDomainDataById_serviceError() throws Exception {
		when(domainDataService.retrieveDetailDomainDataRecordById("finance", "boom"))
				.thenThrow(new DomainDataServiceException("Domain fetch failed"));

		mockMvc.perform(
				get(ENDPOINT+"/my-domain-data").header("Authorization", AUTH).header("X-Id", "boom").param("domainName", "finance"))
				.andExpect(status().isInternalServerError())
				.andExpect(jsonPath("$.message").value("Domain fetch failed"));

		verify(auditService).logAudit(any(AuditDTO.class), eq(500), eq("Domain fetch failed"), eq(AUTH));
	}
}

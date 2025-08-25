package sg.edu.nus.iss.edgp.workflow.management.controller;

import java.util.*;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

import sg.edu.nus.iss.edgp.workflow.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.strategy.impl.ValidationStrategy;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.enums.HTTPVerb;

@WebMvcTest(WorkflowController.class)
@AutoConfigureMockMvc(addFilters = false)
class WorkflowControllerMockBeanTest {

	private static final String AUTH = "Bearer good.jwt.token";
	private static final String FILE_ID = "file-123";
	private static final String ENDPOINT = "/api/wfm";

	@Autowired
	private MockMvc mockMvc;
	@Autowired
	private WorkflowController controller;

	@MockitoBean
	private JWTService jwtService;
	@MockitoBean
	private AuditService auditService;
	@MockitoBean
	private ValidationStrategy validationStrategy;
	@MockitoBean
	private WorkflowService workflowService;

	@BeforeEach
	void boot() {
		// Ensure controller fields used in code are set for the real bean
		ReflectionTestUtils.setField(controller, "activityTypePrefix", "WFM - ");
		ReflectionTestUtils.setField(controller, "genericErrorMessage", "Something went wrong");

		when(jwtService.extractUserIdFromToken("good.jwt.token")).thenReturn("user-1");
		when(jwtService.extractOrgIdFromToken("good.jwt.token")).thenReturn("org-1");

		ValidationResult ok = mock(ValidationResult.class);
		when(ok.isValid()).thenReturn(true);
		when(ok.getStatus()).thenReturn(HttpStatus.OK);
		when(ok.getMessage()).thenReturn("OK");
		when(validationStrategy.isUserOrganizationActive(eq("org-1"), eq(AUTH))).thenReturn(ok);

		when(auditService.createAuditDTO(anyString(), anyString(), anyString(), anyString(), any(HTTPVerb.class)))
				.thenReturn(Mockito.mock(AuditDTO.class));
		doNothing().when(auditService).logAudit(any(AuditDTO.class), anyInt(), anyString(), anyString());
	}

	@Test
	void success() throws Exception {
		List<Map<String, Object>> result = new ArrayList<>();
		Map<String, Object> row = new HashMap<>();
		row.put("id", 1);
		row.put("name", "alpha");
		result.add(row);
		Map<String, Object> summary = new HashMap<>();
		summary.put("totalCount", 10);
		summary.put("successRecords", 8);
		summary.put("failedRecords", 2);
		result.add(summary);

		when(workflowService.retrieveDataList(eq(FILE_ID), any(SearchRequest.class), eq("org-1"))).thenReturn(result);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID).param("page", "1")
				.param("size", "10")).andExpect(status().isOk())
				.andExpect(jsonPath("$.message").value("Successfully retrieved all data list."))
				.andExpect(jsonPath("$.data.successRecords").value(8))
				.andExpect(jsonPath("$.data.failedRecords").value(2))
				.andExpect(jsonPath("$.data.dataRecords[0].id").value(1))
				.andExpect(content().string(Matchers.containsString("\"totalRecord\":10"))).andDo(print());
		verify(auditService, atLeastOnce()).logAudit(any(AuditDTO.class), eq(200), anyString(), eq(AUTH));
	}

	@Test
	void empty() throws Exception {
		when(workflowService.retrieveDataList(eq(FILE_ID), any(SearchRequest.class), eq("org-1")))
				.thenReturn(Collections.emptyList());

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID).param("page", "1")
				.param("size", "10")).andExpect(status().isOk()).andExpect(jsonPath("$.message").value("No data List."))
				.andExpect(jsonPath("$.data").isMap()).andExpect(jsonPath("$.data", Matchers.aMapWithSize(0)));
		verify(auditService).logAudit(any(AuditDTO.class), eq(200), anyString(), eq(AUTH));
	}

	@Test
	void validationFailure() throws Exception {
		ValidationResult invalid = mock(ValidationResult.class);
		when(invalid.isValid()).thenReturn(false);
		when(invalid.getStatus()).thenReturn(HttpStatus.FORBIDDEN);
		when(invalid.getMessage()).thenReturn("Organization is inactive");
		when(validationStrategy.isUserOrganizationActive(eq("org-1"), eq(AUTH))).thenReturn(invalid);

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID).param("page", "1")
				.param("size", "20")).andExpect(status().isForbidden())
				.andExpect(jsonPath("$.message").value("Organization is inactive")).andDo(print());
		verify(auditService).logAudit(any(AuditDTO.class), eq(403), eq("Organization is inactive"), eq(AUTH));
		verifyNoInteractions(workflowService);
	}

	@Test
	@DisplayName("500 error when service throws (with @MockBean)")
	void serviceException() throws Exception {
		when(workflowService.retrieveDataList(eq(FILE_ID), any(SearchRequest.class), eq("org-1")))
				.thenThrow(new WorkflowServiceException("Workflow retrieval failed"));

		mockMvc.perform(get(ENDPOINT).header("Authorization", AUTH).header("X-FileId", FILE_ID).param("page", "1")
				.param("size", "10")).andExpect(status().isInternalServerError())
				.andExpect(jsonPath("$.message").value("Workflow retrieval failed")).andDo(print());
		verify(auditService).logAudit(any(AuditDTO.class), eq(500), eq("Workflow retrieval failed"), eq(AUTH));
	}
}
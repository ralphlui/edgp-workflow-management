package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import sg.edu.nus.iss.edgp.workflow.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.workflow.management.enums.AuditResponseStatus;
import sg.edu.nus.iss.edgp.workflow.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;

public class AuditServiceTest {

	@InjectMocks
	private AuditService auditService;

	@Mock
	private SQSPublishingService sqsPublishingService;

	@Mock
	private JWTService jwtService;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
	}

	@Test
	void testCreateAuditDTO() {
		AuditDTO dto = auditService.createAuditDTO("101", "login", "PREFIX_", "/login", HTTPVerb.GET);
		dto.setUsername("alice");
		assert dto.getUsername().equals("alice");
		assert dto.getRequestType().equals(HTTPVerb.GET);
	}

	@Test
	void testLogAuditSuccess() {
		AuditDTO dto = new AuditDTO();
		String token = "Bearer valid.jwt.token";

		when(jwtService.extractUserNameFromToken("valid.jwt.token")).thenReturn("testUser");
		when(jwtService.extractUserIdFromToken("valid.jwt.token")).thenReturn("123");

		auditService.logAudit(dto, 200, "Success message", token);

		assert dto.getStatusCode() == 200;
		assert dto.getResponseStatus() == AuditResponseStatus.SUCCESS;
		assert dto.getActivityDescription().equals("Success message");

		verify(sqsPublishingService).sendMessage(any(AuditDTO.class));
	}

	@Test
	void testLogAuditFailure() {
		AuditDTO dto = new AuditDTO();
		String token = "Bearer invalid.jwt.token";

		when(jwtService.extractUserNameFromToken("invalid.jwt.token")).thenReturn(null);
		when(jwtService.extractUserIdFromToken("invalid.jwt.token")).thenReturn(null);

		auditService.logAudit(dto, 400, "Error message", token);

		assert dto.getStatusCode() == 400;
		assert dto.getResponseStatus() == AuditResponseStatus.FAILED;
		assert dto.getActivityDescription().equals("Error message");

		verify(sqsPublishingService).sendMessage(any(AuditDTO.class));
	}
}

package sg.edu.nus.iss.edgp.workflow.management.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.APIResponse;
import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/wfm")
@Validated
public class WorkflowController {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowController.class);	 
	private final WorkflowService workflowService;
	private final AuditService auditService;
	private final JWTService jwtService;
	
	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;
	
	@GetMapping(value = "", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> retrievePolicyList(
			@RequestHeader("Authorization") String authorizationHeader, @RequestHeader("X-FileId") String fileId,
			@Valid @ModelAttribute SearchRequest searchRequest) {

		logger.info("Call data list API with page={}, size={}", searchRequest.getPage(), searchRequest.getSize());
		String message = "";
		String activityType = "Retrieve Data List";
		String endpoint = "/api/wfm";
		activityTypePrefix = activityTypePrefix.trim() + activityType;
		String jwtToken = authorizationHeader.substring(7);
		String userId = Optional.ofNullable(jwtService.extractUserIdFromToken(jwtToken)).orElse("Invalid UserId");
		AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint, HTTPVerb.GET);


		try {
			
			List<Map<String, Object>> resultMap = workflowService.retrieveDataList(fileId, searchRequest.getStatus(), searchRequest);
			logger.info("all data list size {}", resultMap.size());

			
			int totalRecord = resultMap.size();
			logger.info("totalRecord: {}", totalRecord);

			if (!resultMap.isEmpty()) {
				message = "Successfully retrieved all data list.";
				logger.info(message);
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK)
						.body(APIResponse.success(resultMap, message, totalRecord));

			} else {
				message = "No data List.";
				logger.info(message);
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK)
						.body(APIResponse.successWithEmptyData(resultMap, message));
			}

		} catch (Exception ex) {
			//message = ex instanceof PolicyServiceException ? ex.getMessage() : genericErrorMessage;
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
	
}

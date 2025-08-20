package sg.edu.nus.iss.edgp.workflow.management.controller;

import java.util.HashMap;
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
import sg.edu.nus.iss.edgp.workflow.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.workflow.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.strategy.impl.ValidationStrategy;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/wfm")
@Validated
public class WorkflowController {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowController.class);
	private final WorkflowService workflowService;
	private final AuditService auditService;
	private final JWTService jwtService;
	private final ValidationStrategy validationStrategy;
	private String genericErrorMessage = "An error occurred while processing your request. Please try again later.";

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	@GetMapping(value = "", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:mdm')")
	public ResponseEntity<APIResponse<Map<String, Object>>> retrievePolicyList(
			@RequestHeader("Authorization") String authorizationHeader, @RequestHeader("X-FileId") String fileId,
			@Valid @ModelAttribute SearchRequest searchRequest) {

		logger.info("Call data list API with page={}, size={}", searchRequest.getPage(), searchRequest.getSize());
		String message = "";
		String activityType = "Retrieve Data List";
		String endpoint = "/api/wfm";
		activityTypePrefix = activityTypePrefix.trim() + activityType;
		String jwtToken = authorizationHeader.substring(7);
		String userId = Optional.ofNullable(jwtService.extractUserIdFromToken(jwtToken)).orElse("Invalid UserId");
		AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint,
				HTTPVerb.GET);

		try {

			String userOrgId = jwtService.extractOrgIdFromToken(jwtToken);
			ValidationResult validationResult = validationStrategy.isUserOrganizationActive(userOrgId,
					authorizationHeader);

			if (!validationResult.isValid()) {
				message = validationResult.getMessage();
				auditService.logAudit(auditDTO, validationResult.getStatus().value(), message, authorizationHeader);
				return ResponseEntity.status(validationResult.getStatus()).body(APIResponse.error(message));
			}

			List<Map<String, Object>> resultMap = workflowService.retrieveDataList(fileId, searchRequest, userOrgId);
			logger.info("all data list size {}", resultMap.size());

			if (resultMap == null || resultMap.isEmpty()) {
				message = "No data List.";
				logger.info(message);
				
				Map<String, Object> finalDataMap = new HashMap<>();
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.ok(APIResponse.successWithEmptyData(finalDataMap, message));
			}

			int totalRecord = (int) resultMap.get(resultMap.size() - 1).get("totalCount");
			int successRecords = (int) resultMap.get(resultMap.size() - 1).get("successRecords");
			int failedRecords = (int) resultMap.get(resultMap.size() - 1).get("failedRecords");
	     	logger.info("totalRecord: {}", totalRecord);
			resultMap.remove(resultMap.size() - 1);

			message = resultMap.isEmpty() ? "No data List" : "Successfully retrieved all data list.";
			totalRecord = resultMap.isEmpty() ? 0 : totalRecord;
			logger.info(message);
			auditService.logAudit(auditDTO, 200, message, authorizationHeader);
			
			Map<String, Object> finalDataMap = new HashMap<>();
			finalDataMap.put("successRecords", successRecords);
			finalDataMap.put("failedRecords", failedRecords);
			finalDataMap.put("dataRecords", resultMap);
			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(finalDataMap, message, totalRecord));

		} catch (Exception ex) {
			message = ex instanceof WorkflowServiceException ? ex.getMessage() : genericErrorMessage;
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

	@GetMapping(value = "/my-data", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:mdm')")
	public ResponseEntity<APIResponse<Map<String, Object>>> getWorkflowDataById(
			@RequestHeader("Authorization") String authorizationHeader,
			@RequestHeader("X-WorkflowId") String workflowId) {
		logger.info("Call viewing poloicy detail by poloicy id...");

		String message = "";
		String activityType = "Retrieve workf flow by id";
		String endpoint = "/api/wfm/my-data";
		activityTypePrefix = activityTypePrefix.trim() + activityType;
		String jwtToken = authorizationHeader.substring(7);
		String userId = Optional.ofNullable(jwtService.extractUserIdFromToken(jwtToken)).orElse("Invalid UserId");
		AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint,
				HTTPVerb.GET);

		try {

			if (workflowId.isEmpty()) {
				message = "Bad Request: Workflow id could not be blank.";
				logger.error(message);
				auditService.logAudit(auditDTO, 400, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(APIResponse.error(message));
			}

			Map<String, Object> dataRecord = workflowService.retrieveDataRecordDetailbyWorkflowId(workflowId);
			String organizationId = (String) dataRecord.get("organization_id");

			String userOrgId = jwtService.extractOrgIdFromToken(jwtToken);
			ValidationResult validationResult = validationStrategy
					.isUserOrganizationValidAndActive(organizationId, userOrgId, authorizationHeader);

			if (!validationResult.isValid()) {
				message = validationResult.getMessage();
				logger.info(message);
				auditService.logAudit(auditDTO, 401, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(APIResponse.error(message));
			}

			message = "Requested data is available.";
			logger.info(message);
			auditService.logAudit(auditDTO, 200, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(dataRecord, message));

		} catch (Exception ex) {
			message = ex instanceof WorkflowServiceException ? ex.getMessage() : genericErrorMessage;
			logger.error(message);
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

}

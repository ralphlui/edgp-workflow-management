package sg.edu.nus.iss.edgp.workflow.management.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
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
import sg.edu.nus.iss.edgp.workflow.management.exception.DomainDataServiceException;
import sg.edu.nus.iss.edgp.workflow.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DomainDataService;
import sg.edu.nus.iss.edgp.workflow.management.strategy.impl.ValidationStrategy;
import sg.edu.nus.iss.edgp.workflow.management.utility.GeneralUtility;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/wfm/domainData")
public class DomainDataController {

	private static final Logger logger = LoggerFactory.getLogger(DomainDataController.class);
	private final AuditService auditService;
	private final JWTService jwtService;
	private final ValidationStrategy validationStrategy;
	private final DomainDataService domainDataService;
	private String genericErrorMessage = "An error occurred while processing your request. Please try again later.";

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	@GetMapping(value = "", produces = "application/json")
	@PreAuthorize(("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:mdm')"))
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> retrievePolicyList(
			@RequestHeader("Authorization") String authorizationHeader, @RequestHeader("X-FileId") String fileId,
			@Valid @ModelAttribute SearchRequest searchRequest) {

		logger.info("Call domain data getAll API with page={}, size={}", searchRequest.getPage(),
				searchRequest.getSize());
		String activityType = "Retrieve Data List";
		String endpoint = "/api/wfm/domainData";
		activityTypePrefix = activityTypePrefix.trim() + activityType;
		String jwtToken = authorizationHeader.substring(7);
		String userId = Optional.ofNullable(jwtService.extractUserIdFromToken(jwtToken)).orElse("Invalid UserId");
		AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint,
				HTTPVerb.GET);
		String message = "";

		try {

			Map<Long, List<Map<String, Object>>> resultMap = new HashMap<>();
			jwtToken = authorizationHeader.substring(7);
			String userOrgId = jwtService.extractOrgIdFromToken(jwtToken);
			String domainName = searchRequest.getDomainName();
			ValidationResult validationResult = validationStrategy.validateDomainAndOrgAccess(domainName, userOrgId,
					authorizationHeader);

			if (!validationResult.isValid()) {
				message = validationResult.getMessage();
				auditService.logAudit(auditDTO, 400, message, authorizationHeader);
				return ResponseEntity.status(validationResult.getStatus()).body(APIResponse.error(message));
			}

			if (searchRequest.getPage() == null) {
				resultMap = domainDataService.retrieveAllDomainDataList(domainName, userOrgId, fileId);
				logger.info("all domain data list size {}", resultMap.size());
			} else {
				Pageable pageable = PageRequest.of(searchRequest.getPage() - 1, searchRequest.getSize(),
						Sort.by("updated_date").ascending());
				resultMap = domainDataService.retrievePaginatedDomainDataList(domainName, userOrgId, fileId, pageable);
				logger.info("paginated domain data list size {}", resultMap.size());
			}

			Map.Entry<Long, List<Map<String, Object>>> firstEntry = resultMap.entrySet().iterator().next();
			long totalRecord = firstEntry.getKey();
			List<Map<String, Object>> domainDataDTOList = firstEntry.getValue();

			logger.info("totalRecord: {}", totalRecord);

			if (!domainDataDTOList.isEmpty()) {
				message = "Successfully retrieved all domain data list.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK)
						.body(APIResponse.success(domainDataDTOList, message, totalRecord));

			} else {
				message = "No Domain Data  List.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK)
						.body(APIResponse.successWithEmptyData(domainDataDTOList, message));
			}

		} catch (Exception ex) {
			message = ex instanceof DomainDataServiceException ? ex.getMessage() : genericErrorMessage;
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

	@GetMapping(value = "/my-domain-data", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:policy') or hasAuthority('SCOPE_view:policy')")
	public ResponseEntity<APIResponse<Map<String, Object>>> getDomainDataById(
			@RequestHeader("Authorization") String authorizationHeader, @RequestHeader("X-Id") String id,
			@Valid @ModelAttribute SearchRequest searchRequest) {
		logger.info("Call retrieving detail domain data record by id...");

		String activityType = "Retrieve Detail Domain Data Record.";
		String endpoint = "/api/wfm/domainData/my-domain-data";
		activityTypePrefix = activityTypePrefix.trim() + activityType;
		String jwtToken = authorizationHeader.substring(7);
		String userId = Optional.ofNullable(jwtService.extractUserIdFromToken(jwtToken)).orElse("Invalid UserId");
		AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint,
				HTTPVerb.GET);
		String message = "";

		try {

			String domainTableName = searchRequest.getDomainName();
			if (!GeneralUtility.hasText(id) || !GeneralUtility.hasText(domainTableName)) {
				message = "Bad Request: Id or domain could not be blank.";
				logger.error(message);
				auditService.logAudit(auditDTO, 400, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(APIResponse.error(message));
			}

			Map<String, Object> detailDomainDataRecord = domainDataService
					.retrieveDetailDomainDataRecordById(domainTableName, id);
			String userOrgId = jwtService.extractOrgIdFromToken(jwtToken);
			String orgId = (String) detailDomainDataRecord.get("organization_id");
			ValidationResult validationResult = validationStrategy.isUserOrganizationValidAndActive(orgId, userOrgId,
					authorizationHeader);

			if (!validationResult.isValid()) {
				message = "Unauthorized to view this domain data record.";
				logger.info(message);
				auditService.logAudit(auditDTO, 401, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(APIResponse.error(message));
			}

			message = "Requested domain data record is available.";
			logger.info(message);
			auditService.logAudit(auditDTO, 200, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(detailDomainDataRecord, message));

		} catch (Exception ex) {
			message = ex instanceof DomainDataServiceException ? ex.getMessage() : genericErrorMessage;
			logger.error(message);
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

}

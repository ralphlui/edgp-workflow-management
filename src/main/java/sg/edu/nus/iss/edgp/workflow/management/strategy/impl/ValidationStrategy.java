package sg.edu.nus.iss.edgp.workflow.management.strategy.impl;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Objects;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.api.connector.OrganizationAPICall;
import sg.edu.nus.iss.edgp.workflow.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.workflow.management.stragtegy.IAPIHelperValidationStrategy;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

@Service
@RequiredArgsConstructor
public class ValidationStrategy implements IAPIHelperValidationStrategy<String> {
	
	private final JSONReader jsonReader;
	private final OrganizationAPICall orgAPICall;
	private static final Logger logger = LoggerFactory.getLogger(ValidationStrategy.class);
	
	public ValidationResult isUserOrganizationActive(String userOrgId, String authHeader) {
	    return validateActive(userOrgId, authHeader);
	}
	

	public ValidationResult isUserOrganizationValidAndActive(String orgId, String userOrgId, String authHeader) {
	   
	    ValidationResult activeCheck = validateActive(userOrgId, authHeader);
	    if (!activeCheck.isValid()) return activeCheck;

	    if (!Objects.equals(userOrgId, orgId)) {
	        return buildInvalidResult("Unauthorized to view this data.");
	    }

	    return activeCheck; 
	}
	
	private ValidationResult validateActive(String userOrgId, String authHeader) {
	    if (isBlank(userOrgId)) {
	        return buildInvalidResult("Organization ID missing or invalid in token");
	    }

	    boolean isActive = Boolean.TRUE.equals(validateActiveOrganization(userOrgId, authHeader));
	    if (!isActive) {
	        return buildInvalidResult("Invalid organization. Unable to view data.");
	    }

	    ValidationResult validationResult = new ValidationResult();
	    validationResult.setValid(true);
	    return validationResult;
	}
	
	
	public ValidationResult validateDomainAndOrgAccess(String domainName, String userOrgId, String authHeader) {
		
		 if (isBlank(domainName)) {
		        return buildInvalidResult("Domain name is missing");
		    }
		
	    return validateActive(userOrgId, authHeader);
	}
	

	/** Tiny utility (avoids external deps). */
	private boolean isBlank(String s) {
	    return s == null || s.trim().isEmpty();
	}

	private ValidationResult buildInvalidResult(String message) {
		ValidationResult result = new ValidationResult();
		result.setMessage(message);
		result.setValid(false);
		result.setStatus(HttpStatus.BAD_REQUEST);
		return result;
	}

	private Boolean validateActiveOrganization(String orgId, String authHeader) {
		String responseStr = orgAPICall.validateActiveOrganization(orgId, authHeader);
		try {
			JSONParser parser = new JSONParser();
			JSONObject jsonResponse = (JSONObject) parser.parse(responseStr);
			Boolean success = jsonReader.getSuccessFromResponse(jsonResponse);

			if (success) {
				JSONObject data = jsonReader.getDataFromResponse(jsonResponse);
				if (data != null) {
					Boolean isActive = (Boolean) data.get("active");
					return isActive;
				}
			}
			return false;

		} catch (ParseException e) {
			logger.error("Error parsing JSON response for validating active organization...", e);
			return false;
		}

	}


}

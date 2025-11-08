package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DynamicSQLRepository;
import sg.edu.nus.iss.edgp.workflow.management.utility.Mode;

@RequiredArgsConstructor
@Service
public class DataRemediationService {

	private static final Logger logger = LoggerFactory.getLogger(DataRemediationService.class);
	private final DynamicSQLRepository dynamicSQLRepository;

	public void updateDataRemediationResponse(Map<String, Object> rawData) {
	    try {
	        if (rawData == null || rawData.isEmpty()) {
	            return; 
	        }

	        // mode must be a String and equal to Mode.AUTO (case-insensitive)
//	        Object modeObj = rawData.get("mode");
//	        if (!(modeObj instanceof String modeStr) || !Mode.auto.toString().equalsIgnoreCase(modeStr)) {
//	            return; // only handle AUTO mode
//	        }

	        logger.info("Data received in updateDataRemediationResponse ");
	        Object dataObj = rawData.get("data");
	        if (!(dataObj instanceof Map<?, ?> data)) {
	            return;
	        }

	        String action      = asString(data.get("action"));
	        String domainName  = asString(data.get("domain_name"));
	        String id          = asString(data.get("id"));
	        String fromValue  = asString(data.get("from_value"));
	        String toValue          = asString(data.get("to_value"));
	        String fieldName          = asString(data.get("field_name"));
	        String message          = asString(data.get("message"));

	        if ("delete".equalsIgnoreCase(action)) {
	            requireNonEmpty(id, "id");
	            requireNonEmpty(domainName, "domain_name");
	            dynamicSQLRepository.insertArchiveData(domainName, id, message);
	            logger.info("Updated remediation delete data status successfully");
	        } else if ("update".equalsIgnoreCase(action)) {
	        	 requireNonEmpty(id, "id");
		         requireNonEmpty(fromValue, "old value");
		         requireNonEmpty(id, "id");
		         requireNonEmpty(toValue, "updated value");
		         requireNonEmpty(fieldName, "column value");
		         dynamicSQLRepository.updateColumnValue(domainName, fieldName, id, fromValue, toValue);
		         logger.info("Updated remediation update data status successfully");
	        }

	    } catch (Exception ex) {
	        logger.error("An error occurred while updating data remediation response", ex);
	        throw new WorkflowServiceException("An error occurred while updating data remediation response", ex);
	    }
	}

	private static String asString(Object o) {
	    return (o == null) ? null : String.valueOf(o);
	}

	private static void requireNonEmpty(String value, String fieldName) {
	    if (value == null || value.isBlank()) {
	        throw new IllegalArgumentException("Missing or empty field: " + fieldName);
	    }
	}
}

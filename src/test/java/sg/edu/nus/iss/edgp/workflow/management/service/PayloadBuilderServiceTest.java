package sg.edu.nus.iss.edgp.workflow.management.service;


import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.PayloadBuilderService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PayloadBuilderServiceTest {

    private PayloadBuilderService service;

    @BeforeEach
    void setUp() {
        service = new PayloadBuilderService(new ObjectMapper());
    }

    @Test
    void buildDataQualityPayLoad_happyPath_wrapsAndStripsReservedKeys() throws Exception {
        
        Map<String, Object> ruleResponse = new HashMap<>();
        ruleResponse.put("data_type", "csv");
        ruleResponse.put("domain_name", "customer");
        ruleResponse.put("file_id", "file-123");
        ruleResponse.put("policy_id", "pol-789");

        
        Map<String, Object> workflowStatus = new HashMap<>();
        workflowStatus.put("created_date", "2025-10-26T10:00:00Z");
        workflowStatus.put("domain_name", "should_be_removed");
        workflowStatus.put("failed_validations", 3);
        workflowStatus.put("file_id", "should_be_removed");
        workflowStatus.put("final_status", "COMPLETED");
        workflowStatus.put("rule_status", "OK");
        workflowStatus.put("dataquality_status", "PASS");
        workflowStatus.put("organization_id", "org-1");
        workflowStatus.put("policy_id", "should_be_removed");
        workflowStatus.put("uploaded_by", "user@example.com");
        workflowStatus.put("staging_id", "stg-1");
     
        workflowStatus.put("records_processed", 120);
        workflowStatus.put("errors", List.of("row 5: email invalid"));

        Map<String, Object> payload = service.buildDataQualityPayLoad(ruleResponse, workflowStatus);
 
        assertNotNull(payload);
        assertTrue(payload.containsKey("data_entry"));

        @SuppressWarnings("unchecked")
        Map<String, Object> dataEntry = (Map<String, Object>) payload.get("data_entry");
 
        assertEquals("csv", dataEntry.get("data_type"));
        assertEquals("customer", dataEntry.get("domain_name"));
        assertEquals("file-123", dataEntry.get("file_id"));
        assertEquals("pol-789", dataEntry.get("policy_id"));

        assertTrue(dataEntry.containsKey("data"));

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) dataEntry.get("data");
        assertNotNull(data);

        assertEquals(120, data.get("records_processed"));
        assertEquals(List.of("row 5: email invalid"), data.get("errors"));
 
        for (String k : List.of(
                "created_date", "domain_name", "failed_validations", "file_id",
                "final_status", "rule_status", "dataquality_status", "organization_id",
                "policy_id", "uploaded_by", "staging_id")) {
            assertFalse(data.containsKey(k), "data should not contain reserved key: " + k);
        }
 
        for (String k : List.of(
                "created_date", "domain_name", "failed_validations", "file_id",
                "final_status", "rule_status", "dataquality_status", "organization_id",
                "policy_id", "uploaded_by", "staging_id")) {
            assertFalse(workflowStatus.containsKey(k), "workflowStatus should have removed: " + k);
        } 
        assertEquals(120, workflowStatus.get("records_processed"));
        assertEquals(List.of("row 5: email invalid"), workflowStatus.get("errors"));
    }

    @Test
    void buildDataQualityPayLoad_missingRuleFields_setsNulls() throws Exception {
        Map<String, Object> ruleResponse = new HashMap<>(); 

        Map<String, Object> workflowStatus = new HashMap<>();
        workflowStatus.put("records_processed", 10);

        Map<String, Object> payload = service.buildDataQualityPayLoad(ruleResponse, workflowStatus);

        @SuppressWarnings("unchecked")
        Map<String, Object> dataEntry = (Map<String, Object>) payload.get("data_entry");

        assertTrue(dataEntry.containsKey("data_type"));
        assertTrue(dataEntry.containsKey("domain_name"));
        assertTrue(dataEntry.containsKey("file_id"));
        assertTrue(dataEntry.containsKey("policy_id"));
 
        assertNull(dataEntry.get("data_type"));
        assertNull(dataEntry.get("domain_name"));
        assertNull(dataEntry.get("file_id"));
        assertNull(dataEntry.get("policy_id"));

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) dataEntry.get("data");
        assertEquals(10, data.get("records_processed"));
    }
}

package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Service
public class PayloadBuilderService {

	private final ObjectMapper mapper;
	public Map<String, Object> buildDataQualityPayLoad(
	        Map<String, Object> ruleResponse,
	        Map<String, Object> workflowStatusData) throws Exception {
	    
	    ObjectNode root = mapper.createObjectNode();
	    ObjectNode entry = root.putObject("data_entry");

	    String data_type = (String) ruleResponse.get("data_type");
	    String domain_name = (String) ruleResponse.get("domain_name");
	    String file_id = (String) ruleResponse.get("file_id");
	    String policy_id = (String) ruleResponse.get("policy_id");

	    entry.put("data_type", data_type);
	    entry.put("domain_name", domain_name);
	    entry.put("file_id", file_id);
	    entry.put("policy_id", policy_id);

	    workflowStatusData.remove("created_date");
	    workflowStatusData.remove("domain_name");
	    workflowStatusData.remove("failed_validations");
	    workflowStatusData.remove("file_id");
	    workflowStatusData.remove("final_status");
	    workflowStatusData.remove("rule_status");
	    workflowStatusData.remove("dataquality_status");
	    //workflowStatusData.remove("organization_id");
	    workflowStatusData.remove("policy_id");
	    workflowStatusData.remove("uploaded_by");
	    workflowStatusData.remove("staging_id");

	    // Convert Map to JsonNode
	    entry.set("data", mapper.valueToTree(workflowStatusData));

	    return mapper.convertValue(root, new com.fasterxml.jackson.core.type.TypeReference<Map<String,Object>>() {});
	}
	}


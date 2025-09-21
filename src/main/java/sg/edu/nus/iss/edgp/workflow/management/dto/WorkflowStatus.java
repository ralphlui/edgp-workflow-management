package sg.edu.nus.iss.edgp.workflow.management.dto;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WorkflowStatus {

	private String id;
	private String ruleStatus;
	private String dataQualityStatus;
	private String finalStatus = "";
	private List<Map<String, Object>> failedValidations;
}

package sg.edu.nus.iss.edgp.workflow.management.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WorkflowStatus {

	private String id;
	private String ruleStatus;
	private String finalStatus = "";
	private String message = "";
}

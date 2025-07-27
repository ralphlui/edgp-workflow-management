package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.service.IWorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;

@RequiredArgsConstructor
@Service
public class WorkflowService implements IWorkflowService {

	private final DynamicDetailService dynamoService;

	@Override
	public void updateWorkflowStatus(Map<String, Object> data) {

		String status = (String) data.get("status");
		String id = (String) data.get("id");
		String fileId = (String) data.get("fileId");
		String message = (String) data.get("message");

		String workflowStatusTable = DynamoConstants.WORK_FLOW_STATUS;
		if (!dynamoService.tableExists(workflowStatusTable)) {
			dynamoService.createTable(workflowStatusTable);
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
		Map<String, String> workflosStatus = new HashMap<String, String>();
		String uploadedDate = LocalDateTime.now().format(formatter);
		workflosStatus.put("id", UUID.randomUUID().toString());
		workflosStatus.put("dataId", id);
		workflosStatus.put("fileId", fileId);
		workflosStatus.put("status", status);
		workflosStatus.put("message", message);
		workflosStatus.put("uploadedDate", uploadedDate);

		dynamoService.insertWorkFlowStatusData(workflowStatusTable, workflosStatus);
	}
}

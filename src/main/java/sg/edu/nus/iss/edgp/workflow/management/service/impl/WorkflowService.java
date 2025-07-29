package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.FileStatus;
import sg.edu.nus.iss.edgp.workflow.management.service.IWorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;
import sg.edu.nus.iss.edgp.workflow.management.utility.FileMetricsConstants;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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
		String totalRowsCount = (String) data.get("totalRowsCount");

		String fileStatusTable = DynamoConstants.FILE_STATUS;
		if (!dynamoService.tableExists(fileStatusTable)) {
			dynamoService.createTable(fileStatusTable);
		}

		Map<String, AttributeValue> file = dynamoService.getFileByFileId(DynamoConstants.FILE_STATUS, fileId);
		if ( file == null || file.isEmpty()) {
			Map<String, String> fileStatus = new HashMap<String, String>();
			fileStatus.put("id", UUID.randomUUID().toString());
			fileStatus.put("fileId", fileId);
			fileStatus.put("totalRowsCount", totalRowsCount);
			fileStatus.put(FileMetricsConstants.PROCESSED_COUNT, "1");
			fileStatus.put(FileMetricsConstants.SUCCESS_COUNT, "0");
			fileStatus.put(FileMetricsConstants.REJECTED_COUNT, "0");
			fileStatus.put(FileMetricsConstants.FAILED_COUNT, "0");
			fileStatus.put(FileMetricsConstants.QUARANTINED_COUNT, "0");

			switch (status) {
			case "S" -> fileStatus.put(FileMetricsConstants.SUCCESS_COUNT, "1");
			case "R" -> fileStatus.put(FileMetricsConstants.REJECTED_COUNT, "1");
			case "F" -> fileStatus.put(FileMetricsConstants.FAILED_COUNT, "1");
			case "Q" -> fileStatus.put(FileMetricsConstants.QUARANTINED_COUNT, "1");
			}

			dynamoService.insertFileStatusData(fileStatusTable, fileStatus);
		} else {

			int successCount = safeParseInt(file.get(FileMetricsConstants.SUCCESS_COUNT), 0);
			int rejectedCount = safeParseInt(file.get(FileMetricsConstants.REJECTED_COUNT), 0);
			int failedCount = safeParseInt(file.get(FileMetricsConstants.FAILED_COUNT), 0);
			int quarantineCount = safeParseInt(file.get(FileMetricsConstants.QUARANTINED_COUNT), 0);
			int processedCount = safeParseInt(file.get(FileMetricsConstants.PROCESSED_COUNT), 0);

			
			FileStatus fileStatus = new FileStatus();
			fileStatus.setFileId(fileId);
			fileStatus.setId(file.get("id").s());
			processedCount += 1;
			fileStatus.setProcessedCount(String.valueOf(processedCount));

			switch (status) {
			case "S" -> successCount++;
			case "R" -> rejectedCount++;
			case "F" -> failedCount++;
			case "Q" -> quarantineCount++;
			}
			
			fileStatus.setSuccessCount(String.valueOf(successCount));		
			fileStatus.setRejectedCount(String.valueOf(rejectedCount));
			fileStatus.setFailedCount(String.valueOf(failedCount));
			fileStatus.setQuarantinedCount(String.valueOf(quarantineCount));
			
			dynamoService.updateFileStatus(fileStatusTable, fileStatus);

		}

		// workflow status table
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
		workflosStatus.put("ruleStatus", status);
		workflosStatus.put("status", status);
		workflosStatus.put("message", message);
		workflosStatus.put("uploadedDate", uploadedDate);

		dynamoService.insertWorkFlowStatusData(workflowStatusTable, workflosStatus);
	}
	
	public static int safeParseInt(AttributeValue attr, int defaultValue) {
	    if (attr == null) return defaultValue;
	    String value = attr.n() != null ? attr.n() : attr.s();
	    if (value == null || value.isEmpty()) return defaultValue;
	    try {
	        return Integer.parseInt(value);
	    } catch (NumberFormatException e) {
	        return defaultValue;
	    }
	}
}

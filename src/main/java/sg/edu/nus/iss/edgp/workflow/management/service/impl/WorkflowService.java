package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.FileStatus;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.service.IWorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;
import sg.edu.nus.iss.edgp.workflow.management.utility.FileMetricsConstants;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@RequiredArgsConstructor
@Service
public class WorkflowService implements IWorkflowService {

	private final DynamicDetailService dynamoService;
	private final ProcessStatusObserverService  processStatusObserverService;

	@Override
	public void updateWorkflowStatus(Map<String, Object> data) {

		String status = (String) data.get("status");
		String workflowStatusId = (String) data.get("id");
		String fileId = (String) data.get("fileId");
		String message = (String) data.get("message");
		String totalRowsCount = (String) data.get("totalRowsCount");

		updateFileStatus(status, fileId, totalRowsCount);

		// workflow status table
		String workflowStatusTable = DynamoConstants.MASTER_DATA_TABLE_NAME;
		if (!dynamoService.tableExists(workflowStatusTable)) {
			dynamoService.createTable(workflowStatusTable);
		}

		Map<String, AttributeValue> workflowStatusData = dynamoService
				.getDataByWorkflowStatusId(DynamoConstants.MASTER_DATA_TABLE_NAME, workflowStatusId);

		if (workflowStatusData == null || workflowStatusData.isEmpty()) {

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
			Map<String, String> workflosStatus = new HashMap<String, String>();
			String uploadedDate = LocalDateTime.now().format(formatter);
			workflosStatus.put("id", UUID.randomUUID().toString());
			workflosStatus.put("workflowStatusId", workflowStatusId);
			workflosStatus.put("fileId", fileId);
			workflosStatus.put("ruleStatus", status);
			workflosStatus.put("status", status);
			workflosStatus.put("message", message);
			workflosStatus.put("uploadedDate", uploadedDate);

			dynamoService.insertWorkFlowStatusData(workflowStatusTable, workflosStatus);

		} else {

			WorkflowStatus workflowStatus = new WorkflowStatus();
			workflowStatus.setFinalStatus(status);
			workflowStatus.setRuleStatus(status);
			workflowStatus.setMessage(message);
			workflowStatus.setId(workflowStatusId);
			dynamoService.updateWorkflowStatus(workflowStatusTable, workflowStatus);
		}

	}

	private void updateFileStatus(String status, String fileId, String totalRowsCount) {

		String fileStatusTable = DynamoConstants.FILE_STATUS;
		if (!dynamoService.tableExists(fileStatusTable)) {
			dynamoService.createTable(fileStatusTable);
		}

		Map<String, AttributeValue> fileStatusData = dynamoService
				.getFileStatusDataByFileId(DynamoConstants.FILE_STATUS, fileId);
		if (fileStatusData == null || fileStatusData.isEmpty()) {
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

			int successCount = safeParseInt(fileStatusData.get(FileMetricsConstants.SUCCESS_COUNT), 0);
			int rejectedCount = safeParseInt(fileStatusData.get(FileMetricsConstants.REJECTED_COUNT), 0);
			int failedCount = safeParseInt(fileStatusData.get(FileMetricsConstants.FAILED_COUNT), 0);
			int quarantineCount = safeParseInt(fileStatusData.get(FileMetricsConstants.QUARANTINED_COUNT), 0);
			int processedCount = safeParseInt(fileStatusData.get(FileMetricsConstants.PROCESSED_COUNT), 0);

			FileStatus fileStatus = new FileStatus();
			fileStatus.setFileId(fileId);
			fileStatus.setId(fileStatusData.get("id").s());
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

	}

	private static int safeParseInt(AttributeValue attr, int defaultValue) {
		if (attr == null)
			return defaultValue;
		String value = attr.n() != null ? attr.n() : attr.s();
		if (value == null || value.isEmpty())
			return defaultValue;
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	@Override
	public boolean isAllDataProcessed(String fileId) {
		return processStatusObserverService.isAllDataProcessed(fileId);
	}
	

}

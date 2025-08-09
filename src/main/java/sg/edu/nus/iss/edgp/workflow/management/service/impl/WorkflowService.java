package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.dto.FileStatus;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.IWorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;
import sg.edu.nus.iss.edgp.workflow.management.utility.FileMetricsConstants;
import sg.edu.nus.iss.edgp.workflow.management.utility.Status;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@RequiredArgsConstructor
@Service
public class WorkflowService implements IWorkflowService {

	private final DynamicDynamoService dynamoService;
	private final DynamicSQLService dynamicSQLService;
	private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);

	@SuppressWarnings("unchecked")
	@Override
	public void updateWorkflowStatus(Map<String, Object> rawData) {

		try {
			String status = (String) rawData.get("status");
			Map<String, Object> data = (Map<String, Object>) rawData.get("data");
			String fileId = (String) rawData.get("file_id");
			String message = (String) rawData.get("message");
			String totalRowsCount = (String) rawData.get("totalRowsCount");
			String workflowStatusId = "";

			if (data != null) {
				workflowStatusId = (String) data.get("id");
				data.remove("id");
				data.put("workflowId", workflowStatusId);
			}

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
				Map<String, String> workflowStatus = new HashMap<String, String>();
				String uploadedDate = LocalDateTime.now().format(formatter);
				putIfNotNull(workflowStatus, "workflowStatusId", workflowStatusId);
				putIfNotNull(workflowStatus, "fileId", fileId);
				putIfNotNull(workflowStatus, "ruleStatus", status);
				putIfNotNull(workflowStatus, "finalStatus", status);
				putIfNotNull(workflowStatus, "message", message);

				// Always present
				workflowStatus.put("id", UUID.randomUUID().toString());
				workflowStatus.put("uploadedDate", uploadedDate);

				dynamoService.insertWorkFlowStatusData(workflowStatusTable, workflowStatus);

			} else {
				WorkflowStatus workflowStatus = new WorkflowStatus();
				workflowStatus.setFinalStatus(status);
				workflowStatus.setRuleStatus(status);
				workflowStatus.setMessage(message);
				workflowStatus.setId(workflowStatusId);
				dynamoService.updateWorkflowStatus(workflowStatusTable, workflowStatus);
			}

			String categoryTableName = (String) rawData.get("category");
			switch (status) {
			case "success" -> dynamicSQLService.buildCreateTableSQL(data, categoryTableName);

			}
		} catch (Exception ex) {
			logger.error("An error occurred while updating workflow status.... {}", ex);
			throw new WorkflowServiceException("An error occurred while updating workflow status", ex);
		}

	}

	private void putIfNotNull(Map<String, String> map, String key, String value) {
		if (value != null) {
			map.put(key, value);
		}
	}

	private void updateFileStatus(String status, String fileId, String totalRowsCount) {

		try {

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

				switch (Status.valueOf(status.toUpperCase())) {
				case SUCCESS -> fileStatus.put(FileMetricsConstants.SUCCESS_COUNT, "1");
				case REJECT -> fileStatus.put(FileMetricsConstants.REJECTED_COUNT, "1");
				case FAIL -> fileStatus.put(FileMetricsConstants.FAILED_COUNT, "1");
				case QUARANTINE -> fileStatus.put(FileMetricsConstants.QUARANTINED_COUNT, "1");
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

				switch (Status.valueOf(status.toUpperCase())) {
				case SUCCESS -> successCount++;
				case REJECT -> rejectedCount++;
				case FAIL -> failedCount++;
				case QUARANTINE -> quarantineCount++;
				}

				fileStatus.setSuccessCount(String.valueOf(successCount));
				fileStatus.setRejectedCount(String.valueOf(rejectedCount));
				fileStatus.setFailedCount(String.valueOf(failedCount));
				fileStatus.setQuarantinedCount(String.valueOf(quarantineCount));

				dynamoService.updateFileStatus(fileStatusTable, fileStatus);

			}

		} catch (Exception ex) {
			logger.error("An error occurred while updating file status.... {}", ex);
			throw new WorkflowServiceException("An error occurred while updating file status", ex);
		}
	}

	@Override
	public List<Map<String, Object>> retrieveDataList(String fileId, String status, SearchRequest searchRequest) {

		try {
			String workflowStatusTable = DynamoConstants.MASTER_DATA_TABLE_NAME;
			Map<String, Object> result = dynamoService.retrieveDataList(workflowStatusTable, fileId, status,
					searchRequest);

			@SuppressWarnings("unchecked")
			List<Map<String, AttributeValue>> items = (List<Map<String, AttributeValue>>) result.get("items");
			Map<String, Object> totalCountMap = new HashMap<>();
			totalCountMap.put("totalCount", result.get("totalCount"));

			List<Map<String, Object>> dynamicList = new ArrayList<>();
			for (Map<String, AttributeValue> item : items) {
				Map<String, Object> dynamicItem = new HashMap<>();

				for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
					String key = entry.getKey();
					AttributeValue value = entry.getValue();

					if (value.s() != null) {
						dynamicItem.put(key, value.s());
					} else if (value.n() != null) {
						dynamicItem.put(key, value.n());
					} else if (value.bool() != null) {
						dynamicItem.put(key, value.bool());

					} else if (value.n() != null) {
						dynamicItem.put(key, value.n());
					} else if (value.hasL()) {
						List<Object> list = new ArrayList<>();
						for (AttributeValue listVal : value.l()) {
							if (listVal.s() != null)
								list.add(listVal.s());
							else if (listVal.n() != null)
								list.add(listVal.n());
							else if (listVal.bool() != null)
								list.add(listVal.bool());
							else
								list.add(listVal.toString()); // fallback
						}
						dynamicItem.put(key, list);
					} else if (value.hasM()) {
						Map<String, Object> nestedMap = new HashMap<>();
						for (Map.Entry<String, AttributeValue> nestedEntry : value.m().entrySet()) {
							AttributeValue nestedVal = nestedEntry.getValue();
							if (nestedVal.s() != null)
								nestedMap.put(nestedEntry.getKey(), nestedVal.s());
							else if (nestedVal.n() != null)
								nestedMap.put(nestedEntry.getKey(), nestedVal.n());
							else if (nestedVal.bool() != null)
								nestedMap.put(nestedEntry.getKey(), nestedVal.bool());
							else
								nestedMap.put(nestedEntry.getKey(), nestedVal.toString()); // fallback
						}
						dynamicItem.put(key, nestedMap);
					} else {
						dynamicItem.put(key, value.toString()); // fallback for unknown types
					}
				}

				dynamicList.add(dynamicItem);
			}
			dynamicList.add(totalCountMap);
			return dynamicList;
		} catch (Exception ex) {
			logger.error("An error occurred while retireving data list.... {}", ex);
			throw new WorkflowServiceException("An error occurred while retireving data list", ex);
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

}

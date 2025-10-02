package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.aws.service.SQSDataQualityRequestService;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.IWorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.Status;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@RequiredArgsConstructor
@Service
public class WorkflowService implements IWorkflowService {

	private final DynamicDynamoService dynamoService;
	private final ProcessStatusObserverService processStatusObserverService;
	private final DynamicSQLService dynamicSQLService;
	private final SQSDataQualityRequestService sqsDataQualityRequestService;
	private final PayloadBuilderService payloadBuilderService;

	private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);

	@Value("${aws.dynamodb.table.master.data.task}")
	private String masterDataTaskTrackerTableName;

	@Value("${aws.dynamodb.table.master.data.header}")
	private String masterDataHeaderTableName;

	@SuppressWarnings("unchecked")
	@Override
	public void updateDataQualityWorkflowStatus(Map<String, Object> rawData) {

		try {
			String status = (String) rawData.get("status");
			Map<String, Object> data = (Map<String, Object>) rawData.get("data");
			List<Map<String, Object>> failedValidations = (List<Map<String, Object>>) rawData.get("failed_validations");
			String workflowStatusId = "";

			if (data != null) {
				workflowStatusId = (String) data.get("id");
			}

			// workflow status table
			if (!dynamoService.tableExists(masterDataTaskTrackerTableName.trim())) {
				dynamoService.createTable(masterDataTaskTrackerTableName.trim());
			}

			Map<String, AttributeValue> workflowStatusData = dynamoService
					.getDataByWorkflowStatusId(masterDataTaskTrackerTableName.trim(), workflowStatusId);

			if (workflowStatusData == null || workflowStatusData.isEmpty()) {

				throw new WorkflowServiceException(
						"Workflow status update aborted: existing workflow status data not found.");

			} else {
				logger.info("Successfully retrieve work flow data by workflow id");
				WorkflowStatus workflowStatus = new WorkflowStatus();
				
				
				Optional.ofNullable(status).filter(s -> !s.isBlank()).ifPresent(s -> {
					workflowStatus.setFinalStatus(s);
					workflowStatus.setDataQualityStatus(s);
				});

				Optional.ofNullable(failedValidations).filter(list -> !list.isEmpty())
						.ifPresent(list -> workflowStatus.setFailedValidations(List.copyOf(list)));

				workflowStatus.setId(workflowStatusId);
				dynamoService.updateWorkflowStatus(masterDataTaskTrackerTableName.trim(), workflowStatus);
				logger.info("Updated data quality workflow status");
				String domainTableName = (String) rawData.get("domain_name");
				insetCleanMasterData(status, domainTableName, workflowStatusData);
				logger.info("Successfully inserted clean data to master data db");

			}
		} catch (Exception ex) {
			logger.error("An error occurred while updating workflow status.... {}", ex);
			throw new WorkflowServiceException("An error occurred while updating workflow status", ex);
		}

	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void updateRuleWorkflowStatus(Map<String, Object> rawData) {

		try {
			String status = (String) rawData.get("status");
			Map<String, Object> data = (Map<String, Object>) rawData.get("data");
			List<Map<String, Object>> failedValidations = (List<Map<String, Object>>) rawData.get("failed_validations");
			String workflowStatusId = "";

			if (data != null) {
				workflowStatusId = (String) data.get("id");
			}

			// workflow status table
			if (!dynamoService.tableExists(masterDataTaskTrackerTableName.trim())) {
				dynamoService.createTable(masterDataTaskTrackerTableName.trim());
			}

			Map<String, AttributeValue> workflowStatusData = dynamoService
					.getDataByWorkflowStatusId(masterDataTaskTrackerTableName.trim(), workflowStatusId);

			if (workflowStatusData == null || workflowStatusData.isEmpty()) {

				throw new WorkflowServiceException(
						"Workflow status update aborted: existing workflow status data not found.");

			} else {
				logger.info("Successfully retrieve work flow data by workflow id");
				WorkflowStatus workflowStatus = new WorkflowStatus();
				Optional.ofNullable(status).filter(s -> !s.isBlank()).ifPresent(s -> {
					workflowStatus.setRuleStatus(s);
					if (s != null && Status.fail.toString().equals(s.toLowerCase())) {
						workflowStatus.setFinalStatus(s);
					}
				});

				Optional.ofNullable(failedValidations).filter(list -> !list.isEmpty())
						.ifPresent(list -> workflowStatus.setFailedValidations(List.copyOf(list)));

				workflowStatus.setId(workflowStatusId);
				dynamoService.updateWorkflowStatus(masterDataTaskTrackerTableName.trim(), workflowStatus);
				logger.info("Updated rule workflow status");
				
				if (status != null && Status.success.toString().equals(status.toLowerCase())) {
					Map<String, Object> workflowStatusFields = dynamoItemToJavaMap(workflowStatusData);
					Map<String, Object> dataQaulityPayLoad = payloadBuilderService.buildDataQualityPayLoad(rawData, workflowStatusFields);
					sqsDataQualityRequestService.forwardToDataQualityRequestQueue(dataQaulityPayLoad);
					
					logger.info("Sent data quality request queue");
					
				}
				

			}
		} catch (Exception ex) {
			logger.error("An error occurred while updating workflow status.... {}", ex);
			throw new WorkflowServiceException("An error occurred while updating workflow status", ex);
		}

	}

	private void insetCleanMasterData(String status, String domainTableName,
			Map<String, AttributeValue> workflowStatusData) {
		if (status != null && Status.success.toString().equals(status.toLowerCase()) && !domainTableName.isEmpty()) {
			Map<String, Object> workflowStatusFields = dynamoItemToJavaMap(workflowStatusData);

			Optional.ofNullable(workflowStatusFields.remove("id"))
					.ifPresent(v -> workflowStatusFields.put("workflow_tracker_id", v));

//			Optional.ofNullable(workflowStatusFields.remove("id"));
			Optional.ofNullable(workflowStatusFields.remove("created_date"));
			Optional.ofNullable(workflowStatusFields.remove("final_status"));
			Optional.ofNullable(workflowStatusFields.remove("rule_status"));
			Optional.ofNullable(workflowStatusFields.remove("staging_id"));
			Optional.ofNullable(workflowStatusFields.remove("failed_validations"));
			Optional.ofNullable(workflowStatusFields.remove("dataquality_status"));
			Optional.ofNullable(workflowStatusFields.remove("domain_name"));
			Optional.ofNullable(workflowStatusFields.remove("policy_id"));
			dynamicSQLService.buildCreateTableSQL(workflowStatusFields, domainTableName);

		}
	}

	@Override
	public List<Map<String, Object>> retrieveDataList(String fileId, SearchRequest searchRequest, String userOrgId) {

		try {
			Map<String, Object> result = dynamoService.retrieveDataList(masterDataTaskTrackerTableName.trim(), fileId,
					searchRequest, userOrgId);

			@SuppressWarnings("unchecked")
			List<Map<String, AttributeValue>> items = (List<Map<String, AttributeValue>>) result.get("items");
			final Map<String, Object> totalCountMap = new HashMap<>(Map.of("totalCount", result.get("totalCount")));
			Optional<String> fileNameOpt = Optional.empty();

			if (fileId != null && !fileId.isBlank()) {
				
				logger.info("Successfully retrieving file data by file id from header table");
				Map<String, AttributeValue> fileRecord = dynamoService
						.getFileDataByFileId(masterDataHeaderTableName.trim(), fileId);

				fileNameOpt = Optional.ofNullable(fileRecord).map(m -> m.get("file_name")).map(AttributeValue::s)
						.filter(s -> !s.isBlank());
			}

			List<Map<String, Object>> dynamicList = new ArrayList<>();
			int successRecords = 0;
			int failedRecords = 0;
			
			for (Map<String, AttributeValue> item : items) {

				Map<String, Object> dynamicItem = dynamoItemToJavaMap(item);
				fileNameOpt.ifPresent(fn -> dynamicItem.put("fileName", fn));
				dynamicList.add(dynamicItem);
				
				String status = dynamicItem.get("final_status").toString();
				if (status != null && status.toLowerCase().equals(Status.success.toString())) {
					successRecords += 1;
				} else if (status != null && status.toLowerCase().equals(Status.fail.toString())) {
					failedRecords += 1;
				}
			}
			totalCountMap.put("successRecords", successRecords);
			totalCountMap.put("failedRecords", failedRecords);
			dynamicList.add(totalCountMap);
			logger.info("Successfully retrieving work flow data list");
			return dynamicList;
		} catch (Exception ex) {
			logger.error("An error occurred while retireving data list.... {}", ex);
			throw new WorkflowServiceException("An error occurred while retireving data list", ex);
		}

	}

	@Override
	public boolean isAllDataProcessed(String fileId) {
		return processStatusObserverService.isAllDataProcessed(fileId);
	}

	@Override
	public Map<String, Object> retrieveDataRecordDetailbyWorkflowId(String workflowStatusId) {

		try {

			Map<String, AttributeValue> workflowStatusData = dynamoService
					.getDataByWorkflowStatusId(masterDataTaskTrackerTableName.trim(), workflowStatusId);

			if (workflowStatusData == null || workflowStatusData.isEmpty()) {
				throw new WorkflowServiceException("No matching data record found");
			}

			logger.info("retrieved data record by workflow id");
			Map<String, Object> dynamicItem = dynamoItemToJavaMap(workflowStatusData);
			logger.info("converted dynamo Item to Map while retireving workflow data record by id");
			return dynamicItem;

		} catch (Exception ex) {
			logger.error("An error occurred while retireving workflow data record by id.... {}", ex);
			throw new WorkflowServiceException("An error occurred while retireving workflow data record by id", ex);
		}

	}

	public Map<String, Object> dynamoItemToJavaMap(Map<String, AttributeValue> itemAttributes) {
		Map<String, Object> plainItem = new HashMap<>();
		for (Map.Entry<String, AttributeValue> attrEntry : itemAttributes.entrySet()) {
			plainItem.put(attrEntry.getKey(), convertAttributeValue(attrEntry.getValue()));
		}
		return plainItem;
	}

	private Object convertAttributeValue(AttributeValue attrValue) {
		if (attrValue.s() != null) {
			return attrValue.s();
		}
		if (attrValue.n() != null) {
			return attrValue.n(); // keep as string, or parse to Number if you want
		}
		if (attrValue.bool() != null) {
			return attrValue.bool();
		}
		if (attrValue.hasL()) {
			List<Object> listValues = new ArrayList<>();
			for (AttributeValue element : attrValue.l()) {
				listValues.add(convertAttributeValue(element)); // recursive call
			}
			return listValues;
		}
		if (attrValue.hasM()) {
			Map<String, Object> mapValue = new HashMap<>();
			for (Map.Entry<String, AttributeValue> mapEntry : attrValue.m().entrySet()) {
				mapValue.put(mapEntry.getKey(), convertAttributeValue(mapEntry.getValue())); // recursive call
			}
			return mapValue;
		}
		if (attrValue.hasSs()) {
			return new ArrayList<>(attrValue.ss());
		}
		if (attrValue.hasNs()) {
			return new ArrayList<>(attrValue.ns());
		}
		if (attrValue.hasBs()) {
			return new ArrayList<>(attrValue.bs());
		}
		return null; // or attrValue.toString() if you want a fallback
	}

}

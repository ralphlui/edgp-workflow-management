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
	private final ProcessStatusObserverService  processStatusObserverService;
	private final DynamicSQLService dynamicSQLService;
	private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);
	
	
	@Value("${aws.dynamodb.table.master.data.task}")
	private String masterDataTaskTrackerTableName;

	@SuppressWarnings("unchecked")
	@Override
	public void updateWorkflowStatus(Map<String, Object> rawData) {

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
				WorkflowStatus workflowStatus = new WorkflowStatus();
				Optional.ofNullable(status).filter(s -> !s.isBlank()).ifPresent(s -> {
					workflowStatus.setFinalStatus(s);
					workflowStatus.setRuleStatus(s);
				});

				Optional.ofNullable(failedValidations).filter(list -> !list.isEmpty())
						.ifPresent(list -> workflowStatus.setFailedValidations(List.copyOf(list)));

				workflowStatus.setId(workflowStatusId);
				dynamoService.updateWorkflowStatus(masterDataTaskTrackerTableName.trim(), workflowStatus);
				String domainTableName = (String) rawData.get("domain_name");
				insetCleanMasterData(status, domainTableName, workflowStatusData);

			}
		} catch (Exception ex) {
			logger.error("An error occurred while updating workflow status.... {}", ex);
			throw new WorkflowServiceException("An error occurred while updating workflow status", ex);
		}

	}

	private void insetCleanMasterData(String status, String domainTableName,
			Map<String, AttributeValue> workflowStatusData) {
		if (status != null && Status.SUCCESS.toString().equals(status.toUpperCase()) && !domainTableName.isEmpty()) {
			Map<String, Object> workflowStatusFields = dynamoItemToJavaMap(workflowStatusData);

			Optional.ofNullable(workflowStatusFields.remove("id"))
					.ifPresent(v -> workflowStatusFields.put("workflowStatusId", v));

			workflowStatusFields.remove("created_date");
			workflowStatusFields.remove("final_status");
			workflowStatusFields.remove("rule_status");
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
			Map<String, Object> totalCountMap = new HashMap<>();
			totalCountMap.put("totalCount", result.get("totalCount"));

			List<Map<String, Object>> dynamicList = new ArrayList<>();
			for (Map<String, AttributeValue> item : items) {

				Map<String, Object> dynamicItem = dynamoItemToJavaMap(item);
				dynamicList.add(dynamicItem);
			}
			dynamicList.add(totalCountMap);
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
	
	
	

	private Map<String, Object> dynamoItemToJavaMap(Map<String, AttributeValue> itemAttributes) {
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

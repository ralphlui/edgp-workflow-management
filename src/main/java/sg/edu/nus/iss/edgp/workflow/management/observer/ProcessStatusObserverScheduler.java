package sg.edu.nus.iss.edgp.workflow.management.observer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.*;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(ProcessStatusObserverScheduler.class);

	private final WorkflowService workflowService;
	private final ProcessStatusObserverService processStatusObserverService;
	private final DynamicDynamoService dynamoService;
	private final DataIngestionNotifierService workflowNotificationService;
	
	@Value("${aws.dynamodb.table.master.data.header}")
	private String masterDataHeaderTableName;
	
	
	@Value("${aws.dynamodb.table.master.data.task}")
	private String masterDataTaskTrackerTableName;

	@Scheduled(fixedDelayString = "PT1M")
	public void checkWorkflowStatus() {
		logger.info("Checking workflow status...");

		try {
			if (dynamoService.tableExists(masterDataHeaderTableName.trim())
					&& dynamoService.tableExists(masterDataTaskTrackerTableName.trim())) {

				// 1) Get the current PROCESSING file
                HashMap<String,String> fileInfo = processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);
                 
                
				if (fileInfo == null || fileInfo.isEmpty()) {
					logger.info("No processing files found.");
					return;

				} else {
					String fileId=fileInfo.get("id").trim();
	                
					boolean isProcessed = workflowService.isAllDataProcessed(fileId);
					if (isProcessed) {
						String fileStatus = processStatusObserverService.getAllStatusForFile(fileId);

						// (2) update file status and file stage as complete
						processStatusObserverService.updateFileStageAndStatus(fileId, FileProcessStage.COMPLETE,
								fileStatus);
						
						//(3) Send notification email
						 
						workflowNotificationService.sendDataIngestionResult(fileInfo);

					}
				}
                
			}else {
				logger.info("No data found to process");
			}
			
		}

		catch (Exception e) {

			logger.error("Unexpected error while polling workflow status.", e);
		}
	}

}

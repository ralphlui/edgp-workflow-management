package sg.edu.nus.iss.edgp.workflow.management.observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.ProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;
import sg.edu.nus.iss.edgp.workflow.management.utility.DynamoConstants;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(ProcessStatusObserverScheduler.class);

	private final WorkflowService workflowService;
	private final ProcessStatusObserverService processStatusObserverService;
	private final DynamicDynamoService dynamoService;

	@Scheduled(fixedDelayString = "PT1M")
	public void checkWorkflowStatus() {
		logger.info("Checking workflow status...");

		try {
			if (dynamoService.tableExists(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim())
					&& dynamoService.tableExists(DynamoConstants.MASTER_DATA_TASK_TRACKER_TABLE_NAME.trim())) {

				// 1) Get the current PROCESSING file
				String fileId = processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);

				if (fileId == null || fileId.isEmpty()) {
					logger.info("No processing files found.");
					return;

				} else {

					boolean isProcessed = workflowService.isAllDataProcessed(fileId);
					if (isProcessed) {
						String fileStatus = processStatusObserverService.getAllStatusForFile(fileId);

						// (2) update file status and file stage as complete
						processStatusObserverService.updateFileStageAndStatus(fileId, FileProcessStage.COMPLETE,
								fileStatus);

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

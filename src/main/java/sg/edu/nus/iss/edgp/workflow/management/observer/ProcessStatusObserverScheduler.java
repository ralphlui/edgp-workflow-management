package sg.edu.nus.iss.edgp.workflow.management.observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.ProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(ProcessStatusObserverScheduler.class);

	private final WorkflowService workflowService;
	private final ProcessStatusObserverService processStatusObserverService;
	

	@Scheduled(fixedDelayString = "PT1M")
	public void checkWorkflowStatusAndPushNext() {
		logger.info("Checking workflow status...");

		try {
			// 1) Get the current PROCESSING file
			String fileId = processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);

			if (fileId.isEmpty()) {
				logger.info("No processing files found.");
				return;

			} else {

				boolean isProcessed = workflowService.isAllDataProcessed(fileId);
				if (isProcessed) {
					boolean fileStatus = processStatusObserverService.isAllTrueForFile(fileId);

					// (2) update file status and file stage as complete
					processStatusObserverService.updateFileStageAndStatus(fileId, FileProcessStage.COMPLETE,
							fileStatus);

				}
			}
		}

		catch (Exception e) {

			logger.error("Unexpected error while polling workflow status.", e);
		}
	}

}

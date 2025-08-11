package sg.edu.nus.iss.edgp.workflow.management.observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.ProcessStatusObserverService;

@RequiredArgsConstructor
@Service
public class ProcessStatusObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(ProcessStatusObserverScheduler.class);

	private final ProcessStatusObserverService processStatusObserverService;

	@Scheduled(fixedRateString = "${polling.interval-ms}")
	public void checkWorkflowStatusAndPushNext() {
		logger.info("Checking workflow status...");

		try {
			    // 1) Get the current PROCESSING file
			String fileId = processStatusObserverService
					.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);

			if (fileId.isEmpty()) {
				logger.info("No processing files found.");
				return;

			} else {
				// (2) check workflow status
				
				boolean isProcessed=processStatusObserverService.isFileProcessed(fileId);
				if(isProcessed) {
					
					// (3) update file stage as complete
					processStatusObserverService.updateFileStage(fileId, FileProcessStage.COMPLETE);

				}
			}
		}

		catch (Exception e) {

			logger.error("Unexpected error while polling workflow status or pushing next batch.", e);
		}
	}

}

package sg.edu.nus.iss.edgp.workflow.management.service;

import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;

public interface IProcessStatusObserverService {
	
    String fetchOldestIdByProcessStage(FileProcessStage stage);
	boolean isFileProcessed(String fileId);
	void updateFileStage(String fileId, FileProcessStage processStage);
}

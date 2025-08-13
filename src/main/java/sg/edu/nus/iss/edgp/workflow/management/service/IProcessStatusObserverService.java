package sg.edu.nus.iss.edgp.workflow.management.service;

import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;

public interface IProcessStatusObserverService {
	
    String fetchOldestIdByProcessStage(FileProcessStage stage);
	boolean isAllDataProcessed(String fileId);
	String getAllStatusForFile(String fileId);
	void updateFileStageAndStatus(String fileId, FileProcessStage stage, String status);
}

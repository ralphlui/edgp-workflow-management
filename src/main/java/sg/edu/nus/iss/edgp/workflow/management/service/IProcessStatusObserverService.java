package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.HashMap;

import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;

public interface IProcessStatusObserverService {
	
	HashMap<String,String> fetchOldestIdByProcessStage(FileProcessStage stage);
	boolean isAllDataProcessed(String fileId);
	String getAllStatusForFile(String fileId);
	void updateFileStageAndStatus(String fileId, FileProcessStage stage, String status);
}

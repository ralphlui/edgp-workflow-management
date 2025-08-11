package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.Optional;

import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;

public interface IProcessStatusObserverService {
	
    Optional<String> fetchOldestIdByProcessStage(FileProcessStage stage);
	boolean isFileProcessed(String fileId);

}

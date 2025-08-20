package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.HashMap;

public interface IDataIngestionNotifierService {
	
	void sendDataIngestionResult(HashMap<String,String> fileInfo);

}

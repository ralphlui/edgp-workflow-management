package sg.edu.nus.iss.edgp.workflow.management.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FileStatus {

	private String id;
	private String fileId;
	private String successCount = "";
	private String rejectedCount = "";
	private String failedCount = "";
	private String quarantinedCount = "";
	private String processedCount = "";
}

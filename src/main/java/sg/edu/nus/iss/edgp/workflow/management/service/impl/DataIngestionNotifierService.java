package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.api.connector.NotificationAPICall;
import sg.edu.nus.iss.edgp.workflow.management.service.IDataIngestionNotifierService;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

import java.io.File;
import java.util.HashMap;

@RequiredArgsConstructor
@Service
public class DataIngestionNotifierService implements IDataIngestionNotifierService{

	private final DynamicDynamoService dynamoService;
	private final NotificationAPICall notificationAPICall;
	private final JSONReader jsonReader;

	@Value("${aws.dynamodb.table.master.data.task}")
	private String masterDataTaskTrackerTableName;

	@Value("${aws.dynamodb.table.master.data.header}")
	private String masterDataHeaderTableName;

	@Override
	public void sendDataIngestionResult(HashMap<String,String> fileInfo) {
		try {
			String fileId=fileInfo.get("id").trim();
            
			File csvFile = dynamoService.exportToCsv(masterDataTaskTrackerTableName, fileInfo);
			if (csvFile != null) {
				String uploadedUser = dynamoService.getUploadUserByFileId(masterDataHeaderTableName, fileId);
				if (uploadedUser != null && !uploadedUser.isEmpty()) {
					String authorizationHeader = jsonReader.getAccessToken(uploadedUser);
					if (authorizationHeader != null && !authorizationHeader.isEmpty()) {
						String orgFileName = csvFile.getAbsoluteFile().getName()
						        .replaceAll("-.*(?=\\.csv$)", "");
						JSONObject response = notificationAPICall.sendEmailWithAttachment(uploadedUser,
								"Data Ingestion Result",
							    "Hi, <br> We are pleased to inform you that your uploaded file <b>" 
							    + orgFileName + "</b> has been successfully ingested. "
							    + "The data ingestion result is attached for your review."
							    + " <br> <br> Thank you for your attention."
							    + "<br><br>"
							    + "<i>(This is an auto-generated email, please do not reply)</i>", 
							    csvFile,
							    authorizationHeader);


						if (!Boolean.TRUE.equals(response.get("success"))) {
							throw new RuntimeException("Email sending failed: " + response.toJSONString());
						}
					}

				}
			}

		} catch (Exception e) {
			throw new RuntimeException("Failed to export workflow CSV and send email", e);
		}
	}
}

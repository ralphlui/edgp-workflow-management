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
						JSONObject response = notificationAPICall.sendEmailWithAttachment(uploadedUser,
								"Data Ingestion Result",
								"Hi <br> Your uploaded file has successfully ingested. Please find attached the data ingestion result.", csvFile,
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

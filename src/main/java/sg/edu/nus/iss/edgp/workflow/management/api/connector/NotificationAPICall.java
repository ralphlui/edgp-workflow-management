package sg.edu.nus.iss.edgp.workflow.management.api.connector;
 

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NotificationAPICall {

    private static final Logger logger = LoggerFactory.getLogger(NotificationAPICall.class);

    @Value("${notification.api.url}")
    private String notificationURL;

    private final RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(5000)
            .setSocketTimeout(5000)
            .build();

    public JSONObject sendEmailWithAttachment(String recipientEmail, String subject, String body, File attachmentFile, String authorizationHeader) {
        JSONObject jsonResponse = new JSONObject();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {

            String url = notificationURL.trim() + "/send-email-with-attachment";
            logger.info("sendEmailWithAttachment URL: {}", url);

            HttpPost request = new HttpPost(url);
            request.setHeader("Authorization", "Bearer "+authorizationHeader);

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
          
            String emailRequestJson = String.format(
                    "{\"userEmail\": \"%s\", \"body\": \"%s\", \"subject\": \"%s\"}",
                    recipientEmail.trim(), body, subject
            );

           
            builder.addTextBody("EmailRequest", emailRequestJson, ContentType.APPLICATION_JSON);

            builder.addBinaryBody("file", attachmentFile, ContentType.DEFAULT_BINARY, attachmentFile.getName());

            HttpEntity multipart = builder.build();
            request.setEntity(multipart);

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                String responseStr = EntityUtils.toString(httpResponse.getEntity(), Charset.forName("UTF-8"));
                logger.info("sendEmailWithAttachment response: {}", responseStr);

                JSONParser parser = new JSONParser();
                jsonResponse = (JSONObject) parser.parse(responseStr);
            } catch (ParseException e) {
                logger.error("JSON parse error: {}", e.toString());
            }

        } catch (IOException e) {
            logger.error("IOException in sendEmailWithAttachment: {}", e.toString());
        } catch (Exception e) {
            logger.error("Exception in sendEmailWithAttachment: {}", e.toString());
        }

        return jsonResponse;
    }
  

}

package sg.edu.nus.iss.edgp.workflow.management;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EdgpWorkflowManagementApplication {

	public static void main(String[] args) {
		SpringApplication.run(EdgpWorkflowManagementApplication.class, args);
	}

}

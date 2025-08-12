package sg.edu.nus.iss.edgp.workflow.management.configuration;


import io.awspring.cloud.sqs.MessageExecutionThreadFactory;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.SqsContainerOptionsBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@Configuration
public class SQSExecutorConfig {

	@Value("${aws.sqs.listener.thread.max.pool.size:5}")
	private int poolSize;

	@Bean
	public SqsAsyncClient sqsAsyncClient() {
		return SqsAsyncClient.builder().region(Region.AP_SOUTHEAST_1).build();
	}

	@Bean
	public TaskExecutor sqsTaskExecutor() {
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.setCorePoolSize(poolSize);
		exec.setMaxPoolSize(poolSize);
		exec.setQueueCapacity(100);
		exec.setThreadNamePrefix("workflow-");
		exec.setThreadFactory(new MessageExecutionThreadFactory("workflow-"));
		exec.initialize();
		return exec;
	}

	@Bean(name = "workflowSqsFactory")
	public SqsMessageListenerContainerFactory<Object> workflowSqsFactory(SqsAsyncClient sqsAsyncClient,
			TaskExecutor sqsTaskExecutor) {

		return SqsMessageListenerContainerFactory
				.builder().sqsAsyncClient(sqsAsyncClient).configure((SqsContainerOptionsBuilder opts) -> opts
						.maxMessagesPerPoll(1).maxConcurrentMessages(1).componentsTaskExecutor(sqsTaskExecutor))
				.build();
	}
}

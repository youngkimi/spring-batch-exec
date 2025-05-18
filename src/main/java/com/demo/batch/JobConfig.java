package com.demo.batch;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class JobConfig {

	private static final Logger logger = LoggerFactory.getLogger(JobConfig.class);


	@Bean
	public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new JobBuilder("job", jobRepository)
			.start(splitFlow(jobRepository, transactionManager))
			.next(simpleStep(jobRepository, transactionManager, 4))
			.build()        //builds FlowJobBuilder instance
			.build();       //builds Job instance
	}


	@Bean
	public Flow splitFlow(
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager
	) {
		return new FlowBuilder<SimpleFlow>("splitFlow")
			.split(taskExecutor())
			.add(
				flow1(jobRepository, transactionManager),
				flow2(jobRepository, transactionManager))
			.build();
	}


	@Bean
	public Flow flow1(
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager
	) {
		return new FlowBuilder<SimpleFlow>("flow1")
			.start(simpleStep(jobRepository, transactionManager, 1))
			.next(simpleStep(jobRepository, transactionManager, 2))
			.build();
	}


	@Bean
	public Flow flow2(
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager
	) {
		return new FlowBuilder<SimpleFlow>("flow2")
			.start(simpleStep(jobRepository, transactionManager, 3))
			.build();
	}


	@Bean
	public TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor("spring_batch");
	}


	Step simpleStep(
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager,
		int step
	) {
		return new StepBuilder("step" + step, jobRepository)
			.<String, String>chunk(2, transactionManager)
			.reader(itemReader(step))
			.writer(itemWriter())
			.build();
	}

	ItemReader<String> itemReader(int step) {
		return new ListItemReader<String>(
			List.of(
				String.format("page 0 of step %d", step),
				String.format("page 1 of step %d", step),
				String.format("page 2 of step %d", step),
				String.format("page 3 of step %d", step)
			)
		);
	}

	ItemWriter<String> itemWriter() {
		return new ListItemWriter<>() {
			@Override
			public void write(Chunk<? extends String> chunk) throws Exception {
				for (String page : chunk) {
					logger.info(page);
				}
				logger.info("++++++++++++++ end of chunk ++++++++++++++");
			}
		};
	}

}

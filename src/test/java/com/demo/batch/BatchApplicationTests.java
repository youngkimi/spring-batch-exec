package com.demo.batch;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.jdbc.Sql;

@Sql({"/schema.sql"})
@SpringBatchTest
@SpringBootTest
class BatchApplicationTests {

	@Autowired
	JobLauncherTestUtils jobLauncherTestUtils;

	@Test
	void 배치_작업은_정의된_흐름대로_정상_종료된다() throws Exception {

		JobExecution jobExecution = jobLauncherTestUtils.launchJob();

		/*
			STEP EXECUTION FLOW

			Step 1 --> Step 2 -->
								|
			Step 3 ------------>--> Step 4

			- Step 1 must complete before Step 2.
			- Step 2 and Step 3 must both complete before Step 4.
			- The flow of (Step 1 -> Step 2) and Step 3 should execute in parallel.
		*/

		assertThat(jobExecution.getExitStatus()).isEqualTo(ExitStatus.COMPLETED);

		List<StepExecution> stepExecutionList = new ArrayList<>(jobExecution.getStepExecutions());

		Map<String, StepExecution> stepMap = stepExecutionList
			.stream()
			.collect(Collectors.toMap(
				StepExecution::getStepName, Function.identity()
			)
		);

		StepExecution step1 = stepMap.get("step1");
		StepExecution step2 = stepMap.get("step2");
		StepExecution step3 = stepMap.get("step3");
		StepExecution step4 = stepMap.get("step4");

		assertThat(step1.getEndTime()).isBefore(step2.getStartTime());
		assertThat(step2.getEndTime()).isBefore(step4.getStartTime());
		assertThat(step3.getEndTime()).isBefore(step4.getStartTime());

		// 겹치는 시간이 있다.
		assertThat(step1.getStartTime()).isBeforeOrEqualTo(step3.getEndTime());
		assertThat(step2.getEndTime()).isAfterOrEqualTo(step3.getStartTime());
	}

}

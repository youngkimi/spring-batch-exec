package com.demo.batch;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
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
	void contextLoads() throws Exception {

		jobLauncherTestUtils.launchJob();
	}

}

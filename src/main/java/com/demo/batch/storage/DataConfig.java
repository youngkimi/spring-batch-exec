package com.demo.batch.storage;

import javax.sql.DataSource;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Configuration
public class DataConfig {

	@Bean
	@ConfigurationProperties(prefix = "storage.datasource")
	HikariConfig hikariConfig() {
		return new HikariConfig();
	}


	@Bean
	HikariDataSource dataSource(
		HikariConfig config
	) {
		return new HikariDataSource(config);
	}


	@Bean
	PlatformTransactionManager transactionManager(
		DataSource dataSource
	) {
		return new DataSourceTransactionManager(dataSource);
	}

}

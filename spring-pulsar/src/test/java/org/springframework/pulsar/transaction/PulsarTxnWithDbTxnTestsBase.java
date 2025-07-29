/*
 * Copyright 2023-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.pulsar.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import javax.sql.DataSource;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;

/**
 * Provides base support for tests that use Pulsar transactions mixed with DB
 * transactions.
 *
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
@Testcontainers(disabledWithoutDocker = true)
class PulsarTxnWithDbTxnTestsBase extends PulsarTxnTestsBase {

	private static final Logger LOG = LoggerFactory.getLogger(PulsarTxnWithDbTxnTestsBase.class);

	static MySQLContainer<?> MYSQL_CONTAINER = new MySQLContainer<>(DockerImageName.parse("mysql:9.2"))
		.withInitScript("transaction/init.sql")
		.withLogConsumer(new Slf4jLogConsumer(LOG));

	@BeforeAll
	static void startMySqlContainer() {
		MYSQL_CONTAINER.start();
	}

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	protected void assertThatMessagesAreInDb(Thing... expectedMessages) {
		assertThatMessagesInDb().contains(expectedMessages);
	}

	protected void assertThatMessagesAreNotInDb(Thing... notExpectedMessages) {
		assertThatMessagesInDb().doesNotContain(notExpectedMessages);
	}

	protected ListAssert<Thing> assertThatMessagesInDb() {
		List<Thing> things = this.jdbcTemplate.query("select * from thing",
				(rs, rowNum) -> new Thing(rs.getLong(1), rs.getString(2)));
		return assertThat(things);
	}

	protected static void insertThingIntoDb(JdbcTemplate jdbcTemplate, Thing thing) {
		jdbcTemplate.update("insert into thing (id, name) values (?, ?)", thing.id(), thing.name());
	}

	@Configuration
	static class TopLevelConfig implements TransactionManagementConfigurer {

		@Bean
		DataSource dataSource() {
			DriverManagerDataSource dataSource = new DriverManagerDataSource();
			dataSource.setDriverClassName(MYSQL_CONTAINER.getDriverClassName());
			dataSource.setUrl(MYSQL_CONTAINER.getJdbcUrl());
			dataSource.setUsername(MYSQL_CONTAINER.getUsername());
			dataSource.setPassword(MYSQL_CONTAINER.getPassword());
			return dataSource;
		}

		@Bean
		JdbcTemplate jdbcTemplate(DataSource dataSource) {
			return new JdbcTemplate(dataSource);
		}

		@Bean
		public DataSourceTransactionManager dataSourceTransactionManager() {
			return new DataSourceTransactionManager(dataSource());
		}

		@Override
		public DataSourceTransactionManager annotationDrivenTransactionManager() {
			return dataSourceTransactionManager();
		}

	}

	public record Thing(Long id, String name) {
	}

}

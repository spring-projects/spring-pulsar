/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.transaction.PulsarTemplateWithDbTransactionTests.PulsarTemplateSynchronizedWithDbTransaction.PulsarTemplateSynchronizedWithDbTransactionConfig;
import org.springframework.pulsar.transaction.PulsarTemplateWithDbTransactionTests.PulsarTemplateSynchronizedWithDbTransaction.PulsarTemplateSynchronizedWithDbTransactionConfig.TestService;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

/**
 * Tests transaction support of {@link PulsarTemplate} when mixed with database
 * transactions.
 *
 * @author Chris Bono
 */
class PulsarTemplateWithDbTransactionTests extends PulsarTxnWithDbTxnTestsBase {

	@Nested
	@ContextConfiguration(classes = PulsarTemplateSynchronizedWithDbTransactionConfig.class)
	class PulsarTemplateSynchronizedWithDbTransaction {

		static final String topic = "ppwdbt-topic";

		@Test
		void whenDbTxnIsCommittedThenMessagesAreCommitted(@Autowired TestService transactionalService) {
			var thing1 = new Thing(100L, "msg1");
			transactionalService.handleRequest(thing1, false, false);
			assertThatMessagesAreInTopic(topic, thing1.name());
			assertThatMessagesAreInDb(thing1);
		}

		@Test
		void whenDbTxnIsSetRollbackOnlyThenMessagesAreNotCommitted(@Autowired TestService transactionalService) {
			var thing2 = new Thing(200L, "msg2");
			transactionalService.handleRequest(thing2, true, false);
			assertThatMessagesAreNotInTopic(topic, thing2.name());
			assertThatMessagesAreNotInDb(thing2);
		}

		@Test
		void whenServiceThrowsExceptionThenMessagesAreNotCommitted(@Autowired TestService transactionalService) {
			var thing3 = new Thing(300L, "msg3");
			assertThatExceptionOfType(PulsarException.class)
				.isThrownBy(() -> transactionalService.handleRequest(thing3, false, true))
				.withMessage("Failed to commit due to chaos");
			assertThatMessagesAreNotInTopic(topic, thing3.name());
			assertThatMessagesAreNotInDb(thing3);
		}

		@EnableTransactionManagement
		@Configuration
		static class PulsarTemplateSynchronizedWithDbTransactionConfig {

			@Service
			class TestService {

				@Autowired
				private JdbcTemplate jdbcTemplate;

				@Autowired
				private PulsarTemplate<String> transactionalPulsarTemplate;

				@Transactional("dataSourceTransactionManager")
				public void handleRequest(Thing thing, boolean setRollbackOnly, boolean throwPulsarException) {
					PulsarTxnWithDbTxnTestsBase.insertThingIntoDb(jdbcTemplate, thing);
					this.transactionalPulsarTemplate.send(topic, thing.name());
					if (setRollbackOnly) {
						TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
					}
					if (throwPulsarException) {
						throw new PulsarException("Failed to commit due to chaos");
					}
				}

			}

		}

	}

}

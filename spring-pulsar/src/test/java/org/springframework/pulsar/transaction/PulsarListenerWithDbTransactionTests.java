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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.transaction.PulsarListenerWithDbTransactionTests.WithDbAndPulsarTransactionCommit.WithDbAndPulsarTransactionCommitConfig;
import org.springframework.pulsar.transaction.PulsarListenerWithDbTransactionTests.WithDbTransactionRollback.WithDbTransactionRollbackConfig;
import org.springframework.pulsar.transaction.PulsarListenerWithDbTransactionTests.WithPulsarTransactionRollback.WithPulsarTransactionRollbackConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

/**
 * Tests transaction support of {@link PulsarListener} when mixed with database
 * transactions.
 *
 * @author Chris Bono
 */
class PulsarListenerWithDbTransactionTests extends PulsarTxnWithDbTxnTestsBase {

	@Nested
	@ContextConfiguration(classes = WithDbAndPulsarTransactionCommitConfig.class)
	class WithDbAndPulsarTransactionCommit {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "plwdbtxn-happy-in";
		static final String topicOut = "plwdbtxn-happy-out";

		@Test
		void whenDbTxnIsCommittedThenMessagesAreCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			var thing = new Thing(1L, "msg1");
			var thingJson = ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(thing);
			nonTransactionalTemplate.send(topicIn, thingJson);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThatMessagesAreInTopic(topicOut, thing.name());
			assertThatMessagesAreInDb(thing);
		}

		@EnableTransactionManagement
		@Configuration(proxyBeanMethods = false)
		static class WithDbAndPulsarTransactionCommitConfig {

			@Autowired
			private JdbcTemplate jdbcTemplate;

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@Transactional("dataSourceTransactionManager")
			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msgJson) throws Exception {
				var thing = ObjectMapperFactory.getMapper().getObjectMapper().readValue(msgJson, Thing.class);
				this.transactionalPulsarTemplate.send(topicOut, thing.name());
				PulsarTxnWithDbTxnTestsBase.insertThingIntoDb(jdbcTemplate, thing);
				latch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithDbTransactionRollbackConfig.class)
	class WithDbTransactionRollback {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "plwdbtxn-dbr-in";
		static final String topicOut = "plwdbtxn-dbr-out";

		@Test
		void whenDbTxnIsSetRollbackOnlyThenMessageCommittedInPulsarButNotInDb() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			var thing = new Thing(2L, "msg2");
			var thingJson = ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(thing);
			nonTransactionalTemplate.send(topicIn, thingJson);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThatMessagesAreNotInDb(thing);
			assertThatMessagesAreInTopic(topicOut, thing.name());
		}

		@EnableTransactionManagement
		@Configuration(proxyBeanMethods = false)
		static class WithDbTransactionRollbackConfig {

			@Autowired
			private JdbcTemplate jdbcTemplate;

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@Transactional("dataSourceTransactionManager")
			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msgJson) throws Exception {
				var thing = ObjectMapperFactory.getMapper().getObjectMapper().readValue(msgJson, Thing.class);
				this.transactionalPulsarTemplate.send(topicOut, thing.name());
				PulsarTxnWithDbTxnTestsBase.insertThingIntoDb(jdbcTemplate, thing);
				TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
				latch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithPulsarTransactionRollbackConfig.class)
	class WithPulsarTransactionRollback {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "plwdbtxn-pr-in";
		static final String topicOut = "plwdbtxn-pr-out";

		@Test
		void whenPulsarTxnIsSetRollbackOnlyThenMessageCommittedInDbButNotInPulsar() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			var thing = new Thing(3L, "msg3");
			var thingJson = ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(thing);
			nonTransactionalTemplate.send(topicIn, thingJson);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThatMessagesAreInDb(thing);
			assertThatMessagesAreNotInTopic(topicOut, thing.name());
		}

		@EnableTransactionManagement
		@Configuration(proxyBeanMethods = false)
		static class WithPulsarTransactionRollbackConfig {

			@Autowired
			private JdbcTemplate jdbcTemplate;

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@Autowired
			private PulsarClient pulsarClient;

			@Transactional("dataSourceTransactionManager")
			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msgJson) throws Exception {
				if (latch.getCount() == 0) {
					return;
				}
				var thing = ObjectMapperFactory.getMapper().getObjectMapper().readValue(msgJson, Thing.class);
				this.transactionalPulsarTemplate.send(topicOut, thing.name());
				PulsarTxnWithDbTxnTestsBase.insertThingIntoDb(jdbcTemplate, thing);
				PulsarTransactionUtils.getResourceHolder(this.pulsarClient).setRollbackOnly();
				latch.countDown();
			}

		}

	}

}

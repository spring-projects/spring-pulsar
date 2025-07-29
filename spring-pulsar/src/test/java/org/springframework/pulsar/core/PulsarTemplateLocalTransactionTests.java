/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.test.support.PulsarConsumerTestUtil;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Tests for {@link PulsarTemplate#executeInTransaction local transactions} in
 * {@code PulsarTemplate}.
 *
 * @author Chris Bono
 */
@Testcontainers(disabledWithoutDocker = true)
class PulsarTemplateLocalTransactionTests {

	private static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage())
		.withTransactions();

	private PulsarClient client;

	@BeforeAll
	static void startContainer() {
		PULSAR_CONTAINER.start();
	}

	@BeforeEach
	void setup() throws PulsarClientException {
		client = PulsarClient.builder()
			.enableTransaction(true)
			.serviceUrl(PULSAR_CONTAINER.getPulsarBrokerUrl())
			.build();
	}

	@AfterEach
	void tearDown() throws PulsarClientException {
		client.close();
	}

	private PulsarTemplate<String> newTransactionalTemplate() {
		var senderFactory = new DefaultPulsarProducerFactory<String>(client, null);
		var pulsarTemplate = new PulsarTemplate<>(senderFactory);
		pulsarTemplate.transactions().setEnabled(true);
		return pulsarTemplate;
	}

	@Test
	void whenTemplateOperationsSucceedThenTxnIsCommitted() {
		String topic = "pttt-send-commit-topic";
		var pulsarTemplate = newTransactionalTemplate();
		var results = pulsarTemplate.executeInTransaction((template) -> {
			var rv = new HashMap<String, MessageId>();
			rv.put("msg1", template.send(topic, "msg1"));
			rv.put("msg2", template.send(topic, "msg2"));
			rv.put("msg3", template.send(topic, "msg3"));
			return rv;
		});
		assertThat(results).containsOnlyKeys("msg1", "msg2", "msg3").allSatisfy((__, v) -> assertThat(v).isNotNull());
		assertMessagesCommitted(topic, List.of("msg1", "msg2", "msg3"));
	}

	@Test
	void whenTemplateOperationsFailThenTxnIsAborted() {
		String topic = "pttt-send-rollback-topic";
		var pulsarTemplate = spy(newTransactionalTemplate());
		doThrow(new PulsarException("5150")).when(pulsarTemplate).send(topic, "msg2");
		assertThatExceptionOfType(PulsarException.class)
			.isThrownBy(() -> pulsarTemplate.executeInTransaction((template) -> {
				var rv = new HashMap<String, MessageId>();
				rv.put("msg1", template.send(topic, "msg1"));
				rv.put("msg2", template.send(topic, "msg2"));
				rv.put("msg3", template.send(topic, "msg3"));
				return rv;
			}))
			.withMessage("5150");
		assertMessagesCommitted(topic, Collections.emptyList());
	}

	@Test
	void transactionsAreIsolatedByThreads() throws Exception {
		String topic = "pttt-send-threads-topic";
		var pulsarTemplate = spy(newTransactionalTemplate());
		doThrow(new PulsarException("5150")).when(pulsarTemplate).send(topic, "msg2");
		var latch = new CountDownLatch(2);
		var t1 = new Thread(() -> {
			pulsarTemplate.executeInTransaction((template) -> template.send(topic, "msg1"));
			latch.countDown();
		});
		var t2 = new Thread(() -> {
			try {
				pulsarTemplate.executeInTransaction((template) -> template.send(topic, "msg2"));
			}
			finally {
				latch.countDown();
			}
		});
		t1.start();
		t2.start();
		assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
		assertMessagesCommitted(topic, List.of("msg1"));
	}

	@Test
	void nestedTransactionsNotAllowed() {
		String topic = "pttt-nested-topic";
		var pulsarTemplate = newTransactionalTemplate();
		assertThatIllegalStateException().isThrownBy(() -> pulsarTemplate.executeInTransaction((template) -> {
			template.send(topic, "msg1");
			template.executeInTransaction((innerTemplate) -> innerTemplate.send(topic, "msg2"));
			return "nope";
		})).withMessage("Nested calls to 'executeInTransaction' are not allowed");
		assertMessagesCommitted(topic, Collections.emptyList());
	}

	@Test
	void transactionsNotAllowedWithNonTransactionalTemplate() {
		var pulsarTemplate = newTransactionalTemplate();
		pulsarTemplate.transactions().setEnabled(false);
		assertThatIllegalStateException().isThrownBy(() -> pulsarTemplate.executeInTransaction((template) -> "boom"))
			.withMessage("This template does not support transactions");
	}

	private void assertMessagesCommitted(String topic, List<String> expectedMsgs) {
		assertThat(PulsarConsumerTestUtil.<String>consumeMessages(client)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(3))
			.get()).map(Message::getValue).containsExactlyInAnyOrderElementsOf(expectedMsgs);
	}

	@Test
	void sendFailsWhenNotInTxnAndAllowNonTxnFlagIsFalse() {
		String topic = "pttt-no-txn-topic";
		var pulsarTemplate = newTransactionalTemplate();
		pulsarTemplate.transactions().setRequired(true);
		assertThatIllegalStateException().isThrownBy(() -> pulsarTemplate.send(topic, "msg1"))
			.withMessageStartingWith("No transaction is in process; possible solutions: run");
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void sendSucceedsWhenNotInTxnAndAllowNonTxnFlagIsNotFalse(boolean shouldExplicitlySetAllowNonTxnFlag) {
		String topic = "pttt-no-txn-%s-topic".formatted(shouldExplicitlySetAllowNonTxnFlag);
		var pulsarTemplate = newTransactionalTemplate();
		if (shouldExplicitlySetAllowNonTxnFlag) {
			pulsarTemplate.transactions().setRequired(false);
		}
		pulsarTemplate.send(topic, "msg1");
		assertMessagesCommitted(topic, List.of("msg1"));
	}

}

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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.BatchListenerWithCommit.BatchListenerWithCommitConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.BatchListenerWithRollback.BatchListenerWithRollbackConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.ListenerWithExternalTransaction.ListenerWithExternalTransactionConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.ListenerWithExternalTransactionRollback.ListenerWithExternalTransactionRollbackConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.RecordListenerWithCommit.RecordListenerWithCommitConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.RecordListenerWithRollback.RecordListenerWithRollbackConfig;
import org.springframework.pulsar.test.support.PulsarConsumerTestUtil;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests for the transaction support in {@link PulsarListener @PulsarListener}.
 *
 * @author Chris Bono
 */
class PulsarListenerTxnTests extends PulsarTxnTestsBase {

	private void assertNoMessagesAvailableInOutputTopic(String topicOut) {
		assertThat(PulsarConsumerTestUtil.<String>consumeMessages(pulsarClient)
			.fromTopic(topicOut)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(7))
			.get()).isEmpty();
	}

	private void assertMessagesAvailableInOutputTopic(String topicOut, String... expectedMessages) {
		this.assertMessagesAvailableInOutputTopic(topicOut, Arrays.stream(expectedMessages).toList());
	}

	private void assertMessagesAvailableInOutputTopic(String topicOut, List<String> expectedMessages) {
		assertThat(PulsarConsumerTestUtil.<String>consumeMessages(pulsarClient)
			.fromTopic(topicOut)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.get()).map(Message::getValue).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	private PulsarTemplate<String> newNonTransactionalTemplate(boolean sendInBatch, int numMessages) {
		List<ProducerBuilderCustomizer<String>> customizers = List.of();
		if (sendInBatch) {
			customizers = List.of((pb) -> pb.enableBatching(true)
				.batchingMaxPublishDelay(2, TimeUnit.SECONDS)
				.batchingMaxMessages(numMessages));
		}
		return new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient, null, customizers));
	}

	@Nested
	@ContextConfiguration(classes = ListenerWithExternalTransactionConfig.class)
	class ListenerWithExternalTransaction {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "pltt-lstnr-ext-txn-in";
		static final String topicOut = "pltt-lstnr-ext-txn-out";

		@Test
		void producedMessageIsCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			nonTransactionalTemplate.send(topicIn, "msg1");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
		}

		@EnablePulsar
		@Configuration
		static class ListenerWithExternalTransactionConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@Transactional
			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msg) {
				transactionalPulsarTemplate.send(topicOut, msg + "-out");
				latch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ListenerWithExternalTransactionRollbackConfig.class)
	class ListenerWithExternalTransactionRollback {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "pltt-lstnr-ext-txn-rb-in";
		static final String topicOut = "pltt-lstnr-ext-txn-rb-out";

		@Test
		void producedMessageIsNotCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			nonTransactionalTemplate.send(topicIn, "msg1");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertNoMessagesAvailableInOutputTopic(topicOut);
		}

		@EnablePulsar
		@Configuration
		static class ListenerWithExternalTransactionRollbackConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@Transactional
			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msg) {
				transactionalPulsarTemplate.send(topicOut, msg + "-out");
				latch.countDown();
				throw new RuntimeException("BOOM");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = RecordListenerWithCommitConfig.class)
	class RecordListenerWithCommit {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "pltt-rec-lstnr-in";
		static final String topicOut = "pltt-rec-lstnr-out";

		@Test
		void producedMessageIsCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			nonTransactionalTemplate.send(topicIn, "msg1");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
		}

		@EnablePulsar
		@Configuration
		static class RecordListenerWithCommitConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msg) {
				transactionalPulsarTemplate.send(topicOut, msg + "-out");
				latch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = RecordListenerWithRollbackConfig.class)
	class RecordListenerWithRollback {

		static final CountDownLatch latch = new CountDownLatch(1);
		static final String topicIn = "pltt-rec-lstnr-rb-in";
		static final String topicOut = "pltt-rec-lstnr-rb-out";

		@Test
		void producedMessageIsNotCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(false, 1);
			nonTransactionalTemplate.send(topicIn, "msg1");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertNoMessagesAvailableInOutputTopic(topicOut);
		}

		@EnablePulsar
		@Configuration
		static class RecordListenerWithRollbackConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@PulsarListener(topics = topicIn, ackMode = AckMode.RECORD)
			void listen(String msg) {
				transactionalPulsarTemplate.send(topicOut, msg + "-out");
				latch.countDown();
				throw new RuntimeException("BOOM-record");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = BatchListenerWithCommitConfig.class)
	class BatchListenerWithCommit {

		static final String topicIn = "pltt-batch-lstnr-in";
		static final String topicOut = "pltt-batch-lstnr-out";
		static final List<String> inputMsgs = List.of("msg1", "msg2", "msg3");
		static final CountDownLatch latch = new CountDownLatch(inputMsgs.size());

		@Test
		void producedMessagesAreCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(true, inputMsgs.size());
			inputMsgs.forEach((msg) -> nonTransactionalTemplate.sendAsync(topicIn, msg));
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toList();
			assertMessagesAvailableInOutputTopic(topicOut, outputMsgs);
		}

		@EnablePulsar
		@Configuration
		static class BatchListenerWithCommitConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@PulsarListener(topics = topicIn, batch = true)
			void listen(List<String> msgs) {
				msgs.forEach((msg) -> {
					transactionalPulsarTemplate.send(topicOut, msg + "-out");
					latch.countDown();
				});
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = BatchListenerWithRollbackConfig.class)
	class BatchListenerWithRollback {

		static final String topicIn = "pltt-batch-lstnr-rb-in";
		static final String topicOut = "pltt-batch-lstnr-rb-out";
		static final List<String> inputMsgs = List.of("msg1", "msg2", "msg3");
		static final CountDownLatch latch = new CountDownLatch(1);

		@Test
		void producedMessagesAreNotCommitted() throws Exception {
			var nonTransactionalTemplate = newNonTransactionalTemplate(true, inputMsgs.size());
			inputMsgs.forEach((msg) -> nonTransactionalTemplate.sendAsync(topicIn, msg));
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertNoMessagesAvailableInOutputTopic(topicOut);
		}

		@EnablePulsar
		@Configuration
		static class BatchListenerWithRollbackConfig {

			@Autowired
			private PulsarTemplate<String> transactionalPulsarTemplate;

			@PulsarListener(topics = topicIn, batch = true)
			void listen(List<String> msgs) {
				msgs.forEach((msg) -> transactionalPulsarTemplate.send(topicOut, msg + "-out"));
				CompletableFuture.runAsync(() -> latch.countDown());
				throw new RuntimeException("BOOM-batch");
			}

		}

	}

}

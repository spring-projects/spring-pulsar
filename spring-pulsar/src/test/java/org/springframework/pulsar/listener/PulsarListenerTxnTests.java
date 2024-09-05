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
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactoryCustomizer;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.BatchListenerWithCommit.BatchListenerWithCommitConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.BatchListenerWithRollback.BatchListenerWithRollbackConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.ListenerWithExternalTransaction.ListenerWithExternalTransactionConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.ListenerWithExternalTransactionRollback.ListenerWithExternalTransactionRollbackConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.RecordListenerWithCommit.RecordListenerWithCommitConfig;
import org.springframework.pulsar.listener.PulsarListenerTxnTests.RecordListenerWithRollback.RecordListenerWithRollbackConfig;
import org.springframework.pulsar.transaction.PulsarTxnTestsBase;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests for the transaction support in {@link PulsarListener @PulsarListener}.
 *
 * @author Chris Bono
 */
class PulsarListenerTxnTests extends PulsarTxnTestsBase {

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
			assertThatMessagesAreInTopic(topicOut, "msg1-out");
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
			assertThatMessagesAreNotInTopic(topicOut, "msg1-out");
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
			assertThatMessagesAreInTopic(topicOut, "msg1-out");
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
			assertThatMessagesAreNotInTopic(topicOut, "msg1-out");
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
			var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toArray(String[]::new);
			assertThatMessagesAreInTopic(topicOut, outputMsgs);
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
			var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toArray(String[]::new);
			assertThatMessagesAreNotInTopic(topicOut, outputMsgs);
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

	@Nested
	class TransactionsDisabledOnListener {

		static final String LISTENER_ID = "disabledOnListenerRequiredOnSettings";

		@Test
		void throwsExceptionWhenTransactionsAreRequired() {
			assertThatIllegalStateException().isThrownBy(() -> {
				var context = new AnnotationConfigApplicationContext();
				context.register(TopLevelConfig.class, TransactionsDisabledOnListenerConfig.class);
				context.registerBean("containerPropsRequiredCustomizer",
						ConcurrentPulsarListenerContainerFactoryCustomizer.class,
						() -> (cf) -> cf.getContainerProperties().transactions().setRequired(true));
				context.refresh();
			}).withMessage("Listener w/ id [%s] requested no transactions but txn are required".formatted(LISTENER_ID));
		}

		@Test
		void disablesTransactionsWhenTransactionsAreNotRequired() {
			try (var context = new AnnotationConfigApplicationContext()) {
				context.register(TopLevelConfig.class, TransactionsDisabledOnListenerConfig.class);
				context.registerBean("containerPropsNotRequiredCustomizer",
						ConcurrentPulsarListenerContainerFactoryCustomizer.class,
						() -> (cf) -> cf.getContainerProperties().transactions().setRequired(false));
				context.refresh();
				var container = context.getBean(PulsarListenerEndpointRegistry.class).getListenerContainer(LISTENER_ID);
				assertThat(container).isNotNull();
				assertThat(container.getContainerProperties()).satisfies((props) -> {
					assertThat(props.transactions().isEnabled()).isFalse();
					assertThat(props.transactions().isRequired()).isFalse();
				});
			}
		}

		static class TransactionsDisabledOnListenerConfig {

			@PulsarListener(id = LISTENER_ID, batch = true, transactional = "false", topics = "not-used")
			void listen(List<String> ignored) {
			}

		}

	}

	@Nested
	class TransactionsEnabledOnListener {

		static final String LISTENER_ID = "enabledOnListener";

		@Test
		void ignoresSettingWhenNoTxnManagerAvailable() {
			assertThatException().isThrownBy(() -> {
				var context = new AnnotationConfigApplicationContext();
				context.register(TopLevelConfig.class, TransactionsEnabledOnListenerConfig.class);
				context.registerBean("removeTxnManagerCustomizer",
						ConcurrentPulsarListenerContainerFactoryCustomizer.class,
						() -> (cf) -> cf.getContainerProperties().transactions().setTransactionManager(null));
				context.refresh();
			})
				.withCauseInstanceOf(IllegalStateException.class)
				.havingRootCause()
				.withMessage("Transactions are enabled but txn manager is not set");
		}

		@Test
		void enablesTransactionsWhenTxnManagerAvailable() {
			try (var context = new AnnotationConfigApplicationContext()) {
				context.register(TopLevelConfig.class, TransactionsEnabledOnListenerConfig.class);
				context.registerBean("containerPropsNotRequiredCustomizer",
						ConcurrentPulsarListenerContainerFactoryCustomizer.class,
						() -> (cf) -> cf.getContainerProperties().transactions().setEnabled(false));
				context.refresh();
				var container = context.getBean(PulsarListenerEndpointRegistry.class).getListenerContainer(LISTENER_ID);
				assertThat(container).isNotNull();
				assertThat(container.getContainerProperties().transactions().isEnabled()).isTrue();
			}
		}

		static class TransactionsEnabledOnListenerConfig {

			@PulsarListener(id = LISTENER_ID, batch = true, transactional = "true", topics = "not-used")
			void listen(List<String> ignored) {
			}

		}

	}

}

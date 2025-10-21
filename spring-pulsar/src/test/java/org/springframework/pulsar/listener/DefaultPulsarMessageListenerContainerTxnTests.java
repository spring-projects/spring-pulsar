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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.pulsar.PulsarContainer;

import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarConsumerTestUtil;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.pulsar.transaction.PulsarTransactionManager;
import org.springframework.pulsar.transaction.PulsarTransactionUtils;

/**
 * Tests for the transaction support in {@link DefaultPulsarMessageListenerContainer}.
 *
 * @author Chris Bono
 */
@Testcontainers(disabledWithoutDocker = true)
class DefaultPulsarMessageListenerContainerTxnTests {

	private static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage())
		.withTransactions();

	private PulsarClient client;

	private PulsarTemplate<String> transactionalPulsarTemplate;

	private PulsarTransactionManager transactionManager;

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
		var producerFactory = new DefaultPulsarProducerFactory<String>(client);
		transactionalPulsarTemplate = new PulsarTemplate<>(producerFactory);
		transactionalPulsarTemplate.transactions().setEnabled(true);
		transactionManager = new PulsarTransactionManager(client);
	}

	@AfterEach
	void tearDown() throws PulsarClientException {
		client.close();
	}

	@Test
	void recordListenerWithAutoRecordAck() throws Exception {
		var topicIn = topicIn("rec-lstnr-auto-rec-ack");
		var topicOut = topicOut("rec-lstnr-auto-rec-ack");
		var containerProps = newContainerProps();
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
	}

	@Test
	void recordListenerWithAutoRecordAckAndRollback() throws Exception {
		var topicIn = topicIn("rec-lstnr-auto-rec-ack-rb");
		var topicOut = topicOut("rec-lstnr-auto-rec-ack-rb");
		var containerProps = newContainerProps();
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void recordListenerWithManualRecordAck() throws Exception {
		var topicIn = topicIn("rec-lstnr-manu-rec-ack");
		var topicOut = topicOut("rec-lstnr-manu-rec-ack");
		var containerProps = newContainerProps();
		containerProps.setAckMode(AckMode.MANUAL);
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarAcknowledgingMessageListener<?>) (consumer, msg, ack) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			ack.acknowledge();
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
	}

	@Test
	void recordListenerWithManualRecordAckAndRollback() throws Exception {
		var topicIn = topicIn("rec-lstnr-manu-rec-ack-rb");
		var topicOut = topicOut("rec-lstnr-manu-rec-ack-rb");
		var containerProps = newContainerProps();
		containerProps.setAckMode(AckMode.MANUAL);
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarAcknowledgingMessageListener<?>) (consumer, msg, ack) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			ack.acknowledge();
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void recordListenerThrowsException() throws Exception {
		var topicIn = topicIn("rec-lstnr-throws-ex");
		var topicOut = topicOut("rec-lstnr-throws-ex");
		var containerProps = newContainerProps();
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			listenerLatch.countDown();
			throw new RuntimeException("BOOM");
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void recordListenerWithNestedTxn() throws Exception {
		var topicIn = topicIn("rec-lstnr-nested-txn");
		var topicOut = topicOut("rec-lstnr-nested-txn");
		var containerProps = newContainerProps();
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.executeInTransaction((t) -> t.send(topicOut, msg.getValue() + "-out"));
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
	}

	@Test
	void recordListenerWithNestedTxnAndRollback() throws Exception {
		var topicIn = topicIn("rec-lstnr-nested-txn-rb");
		var topicOut = topicOut("rec-lstnr-nested-txn-rb");
		var containerProps = newContainerProps();
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.executeInTransaction((t) -> t.send(topicOut, msg.getValue() + "-out"));
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, "msg1");
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
	}

	@Test
	void recordListenerWithMultipleMessages() throws Exception {
		var topicIn = topicIn("rec-lstnr-multi-msg");
		var topicOut = topicOut("rec-lstnr-multi-msg");
		var containerProps = newContainerProps();
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(inputMsgs.size());
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			listenerLatch.countDown();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, inputMsgs);
		var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toList();
		assertMessagesAvailableInOutputTopic(topicOut, outputMsgs);
	}

	@Test
	void recordListenerWithMultipleMessagesAndRollback() throws Exception {
		var topicIn = topicIn("rec-lstnr-multi-msg-rb");
		var topicOut = topicOut("rec-lstnr-multi-msg-rb");
		var containerProps = newContainerProps();
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(inputMsgs.size());
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
			listenerLatch.countDown();
			if (msg.getValue().equals("msg2")) {
				throw new RuntimeException("BOOM-msg2");
			}
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, false, inputMsgs);
		// msg2 txn aborted but msg1 and msg2 txns should have committed
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out", "msg3-out");
	}

	@Test
	void recordListenerWithBatchAckNotSupported() {
		var containerProps = newContainerProps();
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			throw new RuntimeException("should never get here");
		});
		var consumerFactory = new DefaultPulsarConsumerFactory<String>(client, List.of());
		var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
		assertThatIllegalStateException().isThrownBy(() -> container.start())
			.havingRootCause()
			.withMessage("Transactional record listeners can not use batch ack mode");
	}

	@Disabled("Flaky test see spring-pulsar/issues/1294")
	@Test
	void batchListenerUsesBatchAckWhenSharedSub() throws Exception {
		batchListenerUsesProperBatchAckForSubscriptionType("batch-lstr-batch-ack", SubscriptionType.Shared);
	}

	@Disabled("Flaky test see spring-pulsar/issues/1294")
	@Test
	void batchListenerUsesCumulativeAckWhenNotSharedSub() throws Exception {
		batchListenerUsesProperBatchAckForSubscriptionType("batch-lstr-cumltv-ack", SubscriptionType.Exclusive);
	}

	private void batchListenerUsesProperBatchAckForSubscriptionType(String topicPrefix,
			SubscriptionType subscriptionType) throws Exception {
		var topicIn = topicIn(topicPrefix);
		var topicOut = topicOut(topicPrefix);
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setSubscriptionType(subscriptionType);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(inputMsgs.size());
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msgs) -> {
			msgs.forEach((msg) -> {
				transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
				listenerLatch.countDown();
			});
		});
		var spyConsumer = startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true,
				inputMsgs);
		var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toList();
		assertMessagesAvailableInOutputTopic(topicOut, outputMsgs);
		if (subscriptionType == SubscriptionType.Shared) {
			verify(spyConsumer).acknowledgeAsync(any(Messages.class), any(Transaction.class));
		}
		else {
			verify(spyConsumer).acknowledgeCumulativeAsync(any(MessageId.class), any(Transaction.class));
		}
	}

	@Test
	void batchListenerThrowsException() throws Exception {
		var topicIn = topicIn("batch-lstr-throws-ex");
		var topicOut = topicOut("batch-lstr-throws-ex");
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.BATCH);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msgs) -> {
			msgs.forEach((msg) -> transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out"));
			CompletableFuture.runAsync(() -> listenerLatch.countDown());
			throw new RuntimeException("NOPE");
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true, inputMsgs);
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void batchListenerWithTxnMarkedForRollback() throws Exception {
		var topicIn = topicIn("batch-lstr-rollback");
		var topicOut = topicOut("batch-lstr-rollback");
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setSubscriptionType(SubscriptionType.Exclusive);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msgs) -> {
			msgs.forEach((msg) -> transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out"));
			listenerLatch.countDown();
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true, inputMsgs);
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void batchListenerWithNestedProduceTxn() throws Exception {
		var topicIn = topicIn("batch-lstr-nested-txn");
		var topicOut = topicOut("batch-lstr-nested-txn");
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.BATCH);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msgs) -> {
			msgs.forEach((msg) -> {
				if (msg.getValue().equals("msg1")) {
					transactionalPulsarTemplate.executeInTransaction((t) -> t.send(topicOut, msg.getValue() + "-out"));
				}
				else {
					transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out");
				}
			});
			listenerLatch.countDown();
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true, inputMsgs);
		// msg2 and msg3 get rollback but nested txn for msg1 gets committed
		assertMessagesAvailableInOutputTopic(topicOut, "msg1-out");
	}

	@Test
	void batchListenerWithManualAck() throws Exception {
		var topicIn = topicIn("batch-lstr-man-ack");
		var topicOut = topicOut("batch-lstr-man-ack");
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.MANUAL);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(inputMsgs.size());
		containerProps.setMessageListener((PulsarBatchAcknowledgingMessageListener<?>) (consumer, msgs, ack) -> {
			msgs.forEach((msg) -> transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out"));
			ack.acknowledge(msgs.stream().map(Message::getMessageId).toList());
			msgs.forEach((__) -> listenerLatch.countDown());
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true, inputMsgs);
		var outputMsgs = inputMsgs.stream().map((m) -> m.concat("-out")).toList();
		assertMessagesAvailableInOutputTopic(topicOut, outputMsgs);
	}

	@Test
	void batchListenerWithManualAckAndRollback() throws Exception {
		var topicIn = topicIn("batch-lstr-man-ack-rb");
		var topicOut = topicOut("batch-lstr-man-ack-rb");
		var containerProps = newContainerProps();
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.MANUAL);
		var inputMsgs = List.of("msg1", "msg2", "msg3");
		var listenerLatch = new CountDownLatch(1);
		containerProps.setMessageListener((PulsarBatchAcknowledgingMessageListener<?>) (consumer, msgs, ack) -> {
			msgs.forEach((msg) -> transactionalPulsarTemplate.send(topicOut, msg.getValue() + "-out"));
			ack.acknowledge(msgs.stream().map(Message::getMessageId).toList());
			listenerLatch.countDown();
			PulsarTransactionUtils.getResourceHolder(client).setRollbackOnly();
		});
		startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, true, inputMsgs);
		assertNoMessagesAvailableInOutputTopic(topicOut);
	}

	@Test
	void txnBatchListenerWithErrorHandlerNotSupported() {
		var containerProps = newContainerProps();
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setBatchListener(true);
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msg) -> {
			throw new RuntimeException("should never get here");
		});
		var consumerFactory = new DefaultPulsarConsumerFactory<String>(client, List.of());
		var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
		container.setPulsarConsumerErrorHandler(mock(PulsarConsumerErrorHandler.class));
		assertThatIllegalStateException().isThrownBy(() -> container.start())
			.havingRootCause()
			.withMessage("Transactional batch listeners do not support custom error handlers");
	}

	private Consumer<String> startContainerAndSendInputsThenWaitForLatch(String topicIn,
			PulsarContainerProperties containerProps, CountDownLatch listenerLatch, boolean sendInBatch,
			String... inputMsgs) throws InterruptedException {
		return this.startContainerAndSendInputsThenWaitForLatch(topicIn, containerProps, listenerLatch, sendInBatch,
				Arrays.stream(inputMsgs).toList());
	}

	private Consumer<String> startContainerAndSendInputsThenWaitForLatch(String topicIn,
			PulsarContainerProperties containerProps, CountDownLatch listenerLatch, boolean sendInBatch,
			List<String> inputMsgs) throws InterruptedException {
		var consumerFactory = new SpyPulsarConsumerFactory(client, List.of((consumerBuilder) -> {
			consumerBuilder.topic(topicIn);
			consumerBuilder.subscriptionName("sub-" + topicIn);
		}));
		var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
		try {
			container.start();
			var nonTransactionalTemplate = newNonTransactionalTemplate(sendInBatch, inputMsgs.size());
			inputMsgs.forEach((msg) -> nonTransactionalTemplate.sendAsync(topicIn, msg));
			assertThat(listenerLatch.await(sendInBatch ? 8 : 5, TimeUnit.SECONDS)).isTrue();
			if (sendInBatch) {
				// Because the latch may fire before exception is thrown - give it a pause
				Thread.sleep(500);
			}
			return consumerFactory.spyConsumer;
		}
		finally {
			container.stop();
		}
	}

	private PulsarTemplate<String> newNonTransactionalTemplate(boolean sendInBatch, int numInBatch) {
		List<ProducerBuilderCustomizer<String>> customizers = List.of();
		if (sendInBatch) {
			customizers = List.of((pb) -> pb.enableBatching(true)
				.batchingMaxPublishDelay(2, TimeUnit.SECONDS)
				.batchingMaxMessages(numInBatch));
		}
		return new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(client, null, customizers));
	}

	private void assertNoMessagesAvailableInOutputTopic(String topicOut) {
		assertThat(PulsarConsumerTestUtil.<String>consumeMessages(client)
			.fromTopic(topicOut)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(7))
			.get()).isEmpty();
	}

	private void assertMessagesAvailableInOutputTopic(String topicOut, String... expectedMessages) {
		this.assertMessagesAvailableInOutputTopic(topicOut, Arrays.stream(expectedMessages).toList());
	}

	private void assertMessagesAvailableInOutputTopic(String topicOut, List<String> expectedMessages) {
		assertThat(PulsarConsumerTestUtil.<String>consumeMessages(client)
			.fromTopic(topicOut)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.get()).map(Message::getValue).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	private PulsarContainerProperties newContainerProps() {
		var containerProps = new PulsarContainerProperties();
		containerProps.setSchema(Schema.STRING);
		containerProps.transactions().setEnabled(true);
		containerProps.transactions().setTransactionManager(transactionManager);
		containerProps.setAckMode(AckMode.RECORD);
		return containerProps;
	}

	private String topicIn(String testInfo) {
		return "dpmlctt-%s-in".formatted(testInfo);
	}

	private String topicOut(String testInfo) {
		return "dpmlctt-%s-out".formatted(testInfo);
	}

	private static final class SpyPulsarConsumerFactory extends DefaultPulsarConsumerFactory<String> {

		Consumer<String> spyConsumer;

		private SpyPulsarConsumerFactory(PulsarClient pulsarClient,
				List<ConsumerBuilderCustomizer<String>> defaultConfigCustomizers) {
			super(pulsarClient, defaultConfigCustomizers);
		}

		@Override
		public Consumer<String> createConsumer(Schema<String> schema, Collection<String> topics,
				String subscriptionName, ConsumerBuilderCustomizer<String> customizer) {
			this.spyConsumer = spy(super.createConsumer(schema, topics, subscriptionName, customizer));
			return this.spyConsumer;
		}

		@Override
		public Consumer<String> createConsumer(Schema<String> schema, Collection<String> topics,
				String subscriptionName, Map<String, String> metadataProperties,
				List<ConsumerBuilderCustomizer<String>> consumerBuilderCustomizers) {
			this.spyConsumer = spy(super.createConsumer(schema, topics, subscriptionName, metadataProperties,
					consumerBuilderCustomizers));
			return this.spyConsumer;
		}

	}

}

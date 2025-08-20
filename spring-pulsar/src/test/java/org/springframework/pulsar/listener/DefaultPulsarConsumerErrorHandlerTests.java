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

package org.springframework.pulsar.listener;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarOperations;
import org.springframework.pulsar.core.PulsarOperations.SendMessageBuilder;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.TypedMessageBuilderCustomizer;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Soby Chacko
 * @author Sauhard Sharma
 */
public class DefaultPulsarConsumerErrorHandlerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void whenHappyPathErrorHandlingForRecordMessageListenerThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-3";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			throw new RuntimeException();
		}, topicName, -1, false);

		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, List.of(0));

			// Calls to listener - 1 initial + 10 retries
			// Calls to DLT producer - 1 message full failure
			verifyContainerBehavior(container, sendMessageBuilderMock, false, 11, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenErrorHandlingForRecordMessageListenerWithTransientErrorThenDontSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-3";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		AtomicInteger count = new AtomicInteger(0);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			int currentCount = count.incrementAndGet();
			if (currentCount <= 3) {
				throw new RuntimeException();
			}
			return new Object();
		}, topicName, -1, false);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 5));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, List.of(0));

			// Calls to listener - 1 initial + 2 retries + 1 final
			// Calls to DLT producer - 0 full failures
			verifyContainerBehavior(container, sendMessageBuilderMock, false, 4, 0);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenEveryOtherRecordThrowsNonTransientExceptionsRecordMessageListenerThenSendAllFailedMessagesToDlt()
			throws Exception {
		var topicName = "default-error-handler-tests-3";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			Message<Integer> message = invocation.getArgument(1);
			if (message.getValue() % 2 == 0) {
				throw new RuntimeException();
			}
			return new Object();
		}, topicName, -1, false);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 5));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 5 records fail - 5 * (1 + 5 max retry) = 30 + 5 records
			// don't fail = 35
			// Calls to DLT producer - 5 messages full failures
			verifyContainerBehavior(container, sendMessageBuilderMock, false, 35, 5);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerFirstOneOnlyErrorAndRecoverThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-4";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Message<Integer> firstMessage = messages.get(0);
			if (firstMessage.getValue() == 0) {
				throw new PulsarBatchListenerFailedException("failed", firstMessage);
			}
			Acknowledgement acknowledgment = invocation.getArgument(2);
			List<MessageId> messageIds = messages.stream().map(Message::getMessageId).collect(Collectors.toList());
			acknowledgment.acknowledge(messageIds);
			return new Object();
		}, topicName, 10, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 1 initial + 10 retries for failure + 1 final for rest
			// of the batch = 12 calls
			// Calls to DLT producer - 1 message full failure
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 12, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerRecordFailsInTheMiddleThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-5";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Acknowledgement acknowledgment = invocation.getArgument(2);
			for (Message<Integer> message : messages) {
				if (message.getValue() == 5) {
					throw new PulsarBatchListenerFailedException("failed", message);
				}
				else {
					acknowledgment.acknowledge(message.getMessageId());
				}
			}
			return new Object();
		}, topicName, 10, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 1 initial + 10 retries for failure + 1 final for rest
			// of the batch = 12 calls
			// Calls to DLT producer - 1 message full failure
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 12, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerRecordFailsTwiceInTheMiddleThenSendBothFailedMessagesToDlt() throws Exception {
		var topicName = "default-error-handler-tests-6";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Acknowledgement acknowledgment = invocation.getArgument(2);
			for (Message<Integer> message : messages) {
				if (message.getValue() == 2 || message.getValue() == 5) {
					throw new PulsarBatchListenerFailedException("failed", message);
				}
				else {
					acknowledgment.acknowledge(message.getMessageId());
				}
			}
			return new Object();
		}, topicName, 10, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 1 initial + 10 retries for first failure + 1 for second
			// half of the batch + 10 retries for the second failure + 1 final for the
			// rest of the batch = 23 calls
			// Calls to DLT producer - 2 messages full failures
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 23, 2);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerRecordFailsInTheMiddleButTransientErrorThenDontSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-7";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		AtomicInteger count = new AtomicInteger(0);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Acknowledgement acknowledgment = invocation.getArgument(2);
			for (Message<Integer> message : messages) {
				if (message.getValue() == 5) {
					int currentCount = count.getAndIncrement();

					if (currentCount < 3) {
						throw new PulsarBatchListenerFailedException("failed", message);
					}
					else {
						acknowledgment.acknowledge(message.getMessageId());
					}
				}
				else {
					acknowledgment.acknowledge(message.getMessageId());
				}
			}
			return new Object();
		}, topicName, 10, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 1 initial + 2 retries + 1 final for the rest of the
			// batch = 4 calls
			// Calls to DLT producer - 0 full failures
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 4, 0);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerFailsTransientErrorFollowedByNonTransientThenSendAllNonTransientFailedMessageToDlt()
			throws Exception {
		var topicName = "default-error-handler-tests-8";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		AtomicInteger count = new AtomicInteger(0);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Acknowledgement acknowledgment = invocation.getArgument(2);
			for (Message<Integer> message : messages) {
				if (message.getValue() == 5) {
					int currentCount = count.getAndIncrement();
					if (currentCount < 3) {
						throw new PulsarBatchListenerFailedException("failed", message);
					}
					else {
						acknowledgment.acknowledge(message.getMessageId());
					}
				}
				else if (message.getValue() == 7) {
					throw new PulsarBatchListenerFailedException("failed", message);
				}
				else {
					acknowledgment.acknowledge(message.getMessageId());
				}
			}
			return new Object();
		}, topicName, 10, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 10));

		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));

			// Calls to listener - 1 initial + 2 retries for transient failure + 1 for
			// second half of the batch + 10 retries for the second failure + 1 final for
			// the rest of the batch = 15 calls
			// Calls to DLT producer - 1 message full failure
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 15, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchRecordListenerOneMessageBatchFailsThenSentToDltProperlyThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-9";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();

		PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock = mock();
		var mockPulsarTemplate = createMockPulsarTemplate(sendMessageBuilderMock);
		var container = buildPulsarContainer(pulsarClient, (invocation) -> {
			List<Message<Integer>> message = invocation.getArgument(1);
			Message<Integer> integerMessage = message.get(0);
			Integer value = integerMessage.getValue();
			if (value == 0) {
				throw new PulsarBatchListenerFailedException("failed", integerMessage);
			}
			Acknowledgement acknowledgment = invocation.getArgument(2);
			List<MessageId> messageIds = new ArrayList<>();
			for (Message<Integer> integerMessage1 : message) {
				messageIds.add(integerMessage1.getMessageId());
			}
			acknowledgment.acknowledge(messageIds);
			return new Object();
		}, topicName, 1, true);
		container.setPulsarConsumerErrorHandler(createErrorHandler(mockPulsarTemplate, 2));

		try {
			container.start();
			// Send single message in batch
			sendMessages(pulsarClient, topicName, List.of(0));

			// Calls to listener - 1 initial + 2 retries for transient failure + 1 for
			// second half of the batch + 10 retries for the second failure + 1 final for
			// the rest of the batch = 15 calls
			// Calls to DLT producer - 1 message full failure
			verifyContainerBehavior(container, sendMessageBuilderMock, true, 3, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@SuppressWarnings("unchecked")
	private DefaultPulsarMessageListenerContainer<Integer> buildPulsarContainer(PulsarClient pulsarClient,
			Answer<Object> answer, String topicName, int maxNumMessages, boolean isBatch) {
		PulsarContainerProperties pulsarContainerProperties;
		PulsarRecordMessageListener<?> pulsarListener;
		if (isBatch) {
			pulsarContainerProperties = buildPulsarContainerPropertiesForBatch(maxNumMessages);
			pulsarListener = mock(PulsarBatchAcknowledgingMessageListener.class);
			doAnswer(answer).when((PulsarBatchAcknowledgingMessageListener<?>) pulsarListener)
				.received(any(Consumer.class), any(List.class), any(Acknowledgement.class));
		}
		else {
			pulsarContainerProperties = buildPulsarContainerPropertiesForSingle();
			pulsarListener = mock(PulsarRecordMessageListener.class);
			doAnswer(answer).when((PulsarRecordMessageListener<?>) pulsarListener)
				.received(any(Consumer.class), any(Message.class));
		}
		pulsarContainerProperties.setMessageListener(pulsarListener);

		var pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of((consumerBuilder) -> {
			consumerBuilder.topic(topicName);
			consumerBuilder.subscriptionName("%s-sub".formatted(topicName));
		}));

		return new DefaultPulsarMessageListenerContainer<>(pulsarConsumerFactory, pulsarContainerProperties);
	}

	private PulsarContainerProperties buildPulsarContainerPropertiesForBatch(int maxNumMessages) {
		var pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setSchema(Schema.INT32);
		pulsarContainerProperties.setAckMode(AckMode.MANUAL);
		pulsarContainerProperties.setBatchListener(true);
		pulsarContainerProperties.setMaxNumMessages(maxNumMessages);
		pulsarContainerProperties.setBatchTimeoutMillis(60_000);
		return pulsarContainerProperties;
	}

	private PulsarContainerProperties buildPulsarContainerPropertiesForSingle() {
		var pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setSchema(Schema.INT32);
		return pulsarContainerProperties;
	}

	@SuppressWarnings("unchecked")
	private PulsarTemplate<Integer> createMockPulsarTemplate(SendMessageBuilder<Integer> sendMessageBuilderMock) {
		PulsarTemplate<Integer> mockPulsarTemplate = mock(RETURNS_DEEP_STUBS);
		when(mockPulsarTemplate.newMessage(any(Integer.class))
			.withTopic(any(String.class))
			.withMessageCustomizer(any(TypedMessageBuilderCustomizer.class))).thenReturn(sendMessageBuilderMock);
		return mockPulsarTemplate;
	}

	private void sendMessages(PulsarClient pulsarClient, String topicName, List<Integer> messages) {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<Integer>(pulsarClient, topicName);
		var pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		messages.forEach(pulsarTemplate::sendAsync);
	}

	@SuppressWarnings("unchecked")
	private void verifyContainerBehavior(DefaultPulsarMessageListenerContainer<?> container,
			PulsarOperations.SendMessageBuilder<Integer> sendMessageBuilderMock, boolean isBatch,
			int expectedReceivedCalls, int expectedSendAsyncCalls) {
		if (isBatch) {
			await().atMost(Duration.ofSeconds(30))
				.untilAsserted(
						() -> verify((PulsarBatchAcknowledgingMessageListener<?>) container.getContainerProperties()
							.getMessageListener(), times(expectedReceivedCalls))
							.received(any(Consumer.class), any(List.class), any(Acknowledgement.class)));
		}
		else {
			await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(
						(PulsarRecordMessageListener<?>) container.getContainerProperties().getMessageListener(),
						times(expectedReceivedCalls))
					.received(any(Consumer.class), any(Message.class)));
		}
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> verify(sendMessageBuilderMock, times(expectedSendAsyncCalls)).sendAsync());
	}

	private DefaultPulsarConsumerErrorHandler<?> createErrorHandler(PulsarTemplate<?> pulsarTemplate, int retries) {
		return new DefaultPulsarConsumerErrorHandler<>(new PulsarDeadLetterPublishingRecoverer<>(pulsarTemplate),
				new FixedBackOff(100, retries));
	}

	private void safeStopContainer(PulsarMessageListenerContainer container) {
		try {
			container.stop();
		}
		catch (Exception ex) {
			logger.warn(ex, "Failed to stop container %s: %s".formatted(container, ex.getMessage()));
		}
	}

}

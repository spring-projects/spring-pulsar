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
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.TypedMessageBuilderCustomizer;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Soby Chacko
 * @author Sauhard Sharma
 * @@author Chris Bono
 */
class DefaultPulsarConsumerErrorHandlerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void whenRecordListenerWithNonTransientErrorThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-1";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarRecordContainer(pulsarClient, topicName, (__) -> {
			throw new RuntimeException();
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, List.of(0));
			// Calls to listener - 1 initial + 10 retries
			// Calls to DLT producer - 1 failure
			verifyRecordContainerBehavior(container, dltSendMsgBuilder, 11, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenRecordListenerWithTransientErrorThenDoNotSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-2";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var count = new AtomicInteger(0);
		var container = buildPulsarRecordContainer(pulsarClient, topicName, (invocation) -> {
			int currentCount = count.incrementAndGet();
			if (currentCount <= 3) {
				throw new RuntimeException();
			}
			return new Object();
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 5);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, List.of(0));
			// Calls to listener - 1 initial failure + 2 retries + 1 final success
			// Calls to DLT producer - 0 failures
			verifyRecordContainerBehavior(container, dltSendMsgBuilder, 4, 0);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenRecordListenerWithEveryOtherNonTransientErrorThenSendAllFailedMessagesToDlt() throws Exception {
		var topicName = "default-error-handler-tests-3";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarRecordContainer(pulsarClient, topicName, (invocation) -> {
			Message<Integer> message = invocation.getArgument(1);
			if (message.getValue() % 2 == 0) {
				throw new RuntimeException();
			}
			return new Object();
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 5);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 5 records succeed and 5 records fail (each w/ 1 initial
			// call + 5 retries) so
			// 5 + (5 * (1 + 5)) = 35
			// Calls to DLT producer - 5 message failures
			verifyRecordContainerBehavior(container, dltSendMsgBuilder, 35, 5);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerWithFirstMsgNonTransientErrorThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-4";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 10, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Message<Integer> firstMessage = messages.get(0);
			if (firstMessage.getValue() == 0) {
				throw new PulsarBatchListenerFailedException("failed", firstMessage);
			}
			Acknowledgement acknowledgment = invocation.getArgument(2);
			List<MessageId> messageIds = messages.stream().map(Message::getMessageId).collect(Collectors.toList());
			acknowledgment.acknowledge(messageIds);
			return new Object();
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 1 initial + 10 retries for failure + 1 final for rest
			// of batch = 12 total
			// Calls to DLT producer - 1 message failure
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 12, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerWithMiddleMsgNonTransientErrorThenSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-5";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 10, (invocation) -> {
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
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 1 initial + 10 retries for failure + 1 final for rest
			// of batch = 12 total
			// Calls to DLT producer - 1 message failure
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 12, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerWithTwoMiddleMsgsWithNonTransientErrorThenSendBothToDlt() throws Exception {
		var topicName = "default-error-handler-tests-6";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 10, (invocation) -> {
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
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 1 initial + 10 retries for first failure + 1 for second
			// half of the batch + 10 retries for the second failure + 1 final for the
			// rest of the batch = 23 calls total
			// Calls to DLT producer - 2 messages failures
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 23, 2);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerWithMiddleMsgTransientErrorThenDoNotSendToDlt() throws Exception {
		var topicName = "default-error-handler-tests-7";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var count = new AtomicInteger(0);
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 10, (invocation) -> {
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
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 1 initial + 2 retries + 1 final for the rest of the
			// batch = 4 calls total
			// Calls to DLT producer - 0 failures
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 4, 0);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerWithTransientFollowedByNonTransientThenSendTransientToDlt() throws Exception {
		var topicName = "default-error-handler-tests-8";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var count = new AtomicInteger(0);
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 10, (invocation) -> {
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
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 10);
		try {
			container.start();
			sendMessages(pulsarClient, topicName, IntStream.range(0, 10).boxed().collect(Collectors.toList()));
			// Calls to listener - 1 initial + 2 retries for transient failure + 1 for
			// second half of the batch + 10 retries for the second failure + 1 final for
			// the rest of the batch = 15 calls total
			// Calls to DLT producer - 1 message full failure
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 15, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenBatchListenerOneMessageBatchFailsThenSentToDlt() throws Exception {
		var topicName = "default-error-handler-tests-9";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var container = buildPulsarBatchContainer(pulsarClient, topicName, 1, (invocation) -> {
			List<Message<Integer>> messages = invocation.getArgument(1);
			Message<Integer> firstMessage = messages.get(0);
			if (firstMessage.getValue() == 0) {
				throw new PulsarBatchListenerFailedException("failed", firstMessage);
			}
			Acknowledgement acknowledgment = invocation.getArgument(2);
			List<MessageId> messageIds = messages.stream().map(Message::getMessageId).collect(Collectors.toList());
			acknowledgment.acknowledge(messageIds);
			return new Object();
		});
		var dltSendMsgBuilder = setRetryDltErrorHandlerOnContainer(container, 2);
		try {
			container.start();
			// Send single message in batch
			sendMessages(pulsarClient, topicName, List.of(0));
			// Initial call should fail
			// Next 2 calls should fail (retries 2)
			// No more calls after that - msg should go to DLT
			verifyBatchContainerBehavior(container, dltSendMsgBuilder, 3, 1);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@SuppressWarnings("unchecked")
	private DefaultPulsarMessageListenerContainer<Integer> buildPulsarRecordContainer(PulsarClient pulsarClient,
			String topicName, Answer<Object> answerWhenMsgReceivedOnListener) {
		PulsarRecordMessageListener<?> pulsarListener = mock();
		doAnswer(answerWhenMsgReceivedOnListener).when(pulsarListener)
			.received(any(Consumer.class), any(Message.class));
		var pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setSchema(Schema.INT32);
		pulsarContainerProperties.setMessageListener(pulsarListener);
		var pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of((consumerBuilder) -> {
			consumerBuilder.topic(topicName);
			consumerBuilder.subscriptionName("%s-sub".formatted(topicName));
		}));
		return new DefaultPulsarMessageListenerContainer<>(pulsarConsumerFactory, pulsarContainerProperties);
	}

	@SuppressWarnings("unchecked")
	private DefaultPulsarMessageListenerContainer<Integer> buildPulsarBatchContainer(PulsarClient pulsarClient,
			String topicName, int maxNumMessages, Answer<Object> answerWhenMsgReceivedOnListener) {
		PulsarBatchAcknowledgingMessageListener<?> pulsarListener = mock();
		doAnswer(answerWhenMsgReceivedOnListener).when(pulsarListener)
			.received(any(Consumer.class), any(List.class), any(Acknowledgement.class));
		var pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setSchema(Schema.INT32);
		pulsarContainerProperties.setAckMode(AckMode.MANUAL);
		pulsarContainerProperties.setBatchListener(true);
		pulsarContainerProperties.setMaxNumMessages(maxNumMessages);
		pulsarContainerProperties.setBatchTimeoutMillis(60_000);
		pulsarContainerProperties.setMessageListener(pulsarListener);
		var pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of((consumerBuilder) -> {
			consumerBuilder.topic(topicName);
			consumerBuilder.subscriptionName("%s-sub".formatted(topicName));
		}));
		return new DefaultPulsarMessageListenerContainer<>(pulsarConsumerFactory, pulsarContainerProperties);
	}

	@SuppressWarnings("unchecked")
	private PulsarOperations.SendMessageBuilder<Integer> setRetryDltErrorHandlerOnContainer(
			DefaultPulsarMessageListenerContainer<?> container, int maxRetries) {
		PulsarOperations.SendMessageBuilder<Integer> dltSendMsgBuilder = mock();
		PulsarTemplate<Integer> dltPulsarTemplate = mock(RETURNS_DEEP_STUBS);
		when(dltPulsarTemplate.newMessage(any(Integer.class))
			.withTopic(any(String.class))
			.withMessageCustomizer(any(TypedMessageBuilderCustomizer.class))).thenReturn(dltSendMsgBuilder);
		var errorHandler = new DefaultPulsarConsumerErrorHandler<>(
				new PulsarDeadLetterPublishingRecoverer<>(dltPulsarTemplate), new FixedBackOff(100, maxRetries));
		container.setPulsarConsumerErrorHandler(errorHandler);
		return dltSendMsgBuilder;
	}

	private void sendMessages(PulsarClient pulsarClient, String topicName, List<Integer> messages) {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<Integer>(pulsarClient, topicName);
		var pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		messages.forEach(pulsarTemplate::sendAsync);
	}

	@SuppressWarnings("unchecked")
	private void verifyRecordContainerBehavior(DefaultPulsarMessageListenerContainer<?> container,
			PulsarOperations.SendMessageBuilder<Integer> dltSendMsgBuilder, int expectedListenerReceivedCalls,
			int expectedDltSendMsgCalls) {
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(
					(PulsarRecordMessageListener<?>) container.getContainerProperties().getMessageListener(),
					times(expectedListenerReceivedCalls))
				.received(any(Consumer.class), any(Message.class)));
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> verify(dltSendMsgBuilder, times(expectedDltSendMsgCalls)).sendAsync());
	}

	@SuppressWarnings("unchecked")
	private void verifyBatchContainerBehavior(DefaultPulsarMessageListenerContainer<?> container,
			PulsarOperations.SendMessageBuilder<Integer> dltSendMsgBuilder, int expectedListenerReceivedCalls,
			int expectedDltSendMsgCalls) {
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> verify((PulsarBatchAcknowledgingMessageListener<?>) container.getContainerProperties()
				.getMessageListener(), times(expectedListenerReceivedCalls))
				.received(any(Consumer.class), any(List.class), any(Acknowledgement.class)));
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> verify(dltSendMsgBuilder, times(expectedDltSendMsgCalls)).sendAsync());
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

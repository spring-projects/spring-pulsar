/*
 * Copyright 2022-2024 the original author or authors.
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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.ConsumerTestUtils;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.JSONSchemaUtil;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.event.ConsumerFailedToStartEvent;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordObjectMapper;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.pulsar.transaction.PulsarAwareTransactionManager;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 */
class DefaultPulsarMessageListenerContainerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void basicDefaultConsumer() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-012");
					consumerBuilder.subscriptionName("dpmlct-sb-012");
				}));
		CountDownLatch latch = new CountDownLatch(1);
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-012");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		pulsarTemplate.sendAsync("hello john doe");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Disabled
	@Test
	void containerPauseAndResumeFeatureUsingWaitAndNotify() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("containerPauseResumeWaitNotify-topic");
					consumerBuilder.subscriptionName("containerPauseResumeWaitNotify-sub");
				}));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);

		Lock reentrantLock = new ReentrantLock();
		Condition lockCondition = reentrantLock.newCondition();

		Lock spyLock = spy(reentrantLock);
		Condition spyCondition = spy(lockCondition);

		ReflectionTestUtils.setField(container, "lockOnPause", spyLock);
		ReflectionTestUtils.setField(container, "pausedCondition", spyCondition);

		CountDownLatch latchOnLockInvocation = new CountDownLatch(2);
		CountDownLatch latchOnUnlockInvocation = new CountDownLatch(2);
		CountDownLatch latchOnAwaitInvocation = new CountDownLatch(1);
		CountDownLatch latchOnSignalInvocation = new CountDownLatch(1);

		doAnswer(invocation -> {
			latchOnLockInvocation.countDown();
			return invocation.callRealMethod();
		}).when(spyLock).lock();

		doAnswer(invocation -> {
			latchOnUnlockInvocation.countDown();
			return invocation.callRealMethod();
		}).when(spyLock).unlock();

		doAnswer(invocation -> {
			latchOnAwaitInvocation.countDown();
			return invocation.callRealMethod();
		}).when(spyCondition).await();

		doAnswer(invocation -> {
			latchOnSignalInvocation.countDown();
			return invocation.callRealMethod();
		}).when(spyCondition).signal();

		container.start();

		container.pause();

		await().until(container::isPaused);

		container.resume();

		await().until(() -> !container.isPaused());

		assertThat(latchOnLockInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnUnlockInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnAwaitInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnSignalInvocation.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		pulsarClient.close();
	}

	@Test
	void subscriptionInitialPositionEarliest() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-013");
					consumerBuilder.subscriptionName("dpmlct-sb-013");
					consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
				}));
		CountDownLatch latch = new CountDownLatch(5);
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-013");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 5; i++) {
			pulsarTemplate.send("hello john doe" + i);
		}
		// Only start container after all the messages are sent
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Test
	void subscriptionInitialPositionDefaultLatest() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-014");
					consumerBuilder.subscriptionName("dpmlct-sb-014");
				}));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		List<String> messages = new ArrayList<>();
		pulsarContainerProperties.setMessageListener(
				(PulsarRecordMessageListener<?>) (consumer, msg) -> messages.add((String) msg.getValue()));
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-014");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 5; i++) {
			pulsarTemplate.send("hello john doe" + i);
		}
		// Only start container after all the messages are sent
		container.start();
		pulsarTemplate.send("hello john doe" + 5);
		Thread.sleep(2_000);
		assertThat(messages.size()).isEqualTo(1);
		assertThat(messages.get(0)).isEqualTo("hello john doe5");
		container.stop();
		pulsarClient.close();
	}

	@Test
	void negativeAckRedeliveryBackoff() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		RedeliveryBackoff redeliveryBackoff = MultiplierRedeliveryBackoff.builder()
			.minDelayMs(1000)
			.maxDelayMs(5 * 1000)
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(
				new DefaultPulsarConsumerFactory<>(pulsarClient, List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-015");
					consumerBuilder.subscriptionName("dpmlct-sb-015");
					consumerBuilder.negativeAckRedeliveryBackoff(redeliveryBackoff);
				})));
		CountDownLatch latch = new CountDownLatch(10);
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			latch.countDown();
			if (((String) msg.getValue()).endsWith("4")) {
				throw new RuntimeException("fail");
			}
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);

		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-015");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 5; i++) {
			pulsarTemplate.send("hello john doe" + i);
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

		// At this point, we should have 6 call to nack. The first send + 5 more resends
		// due to the backoff setting and the above latch now counted down to zero.
		// There may be a race condition, the below assertion find an extra nack,
		// but the probability for that is low as we have a long enough backoff
		// multiplier.
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, times(6)).negativeAcknowledge(any(Message.class)));

		container.stop();
		pulsarClient.close();
	}

	@Test
	void deadLetterPolicyDefault() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
			.maxRedeliverCount(1)
			.deadLetterTopic("dpmlct-016-dlq-topic")
			.build();
		DefaultPulsarConsumerFactory<Integer> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-016");
					consumerBuilder.subscriptionName("dpmlct-sb-016");
					consumerBuilder.negativeAckRedeliveryDelay(1L, TimeUnit.SECONDS);
					consumerBuilder.deadLetterPolicy(deadLetterPolicy);
				}));

		CountDownLatch dlqLatch = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(6);

		PulsarContainerProperties dlqContainerProperties = new PulsarContainerProperties();
		dlqContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> dlqLatch.countDown());
		dlqContainerProperties.setSchema(Schema.INT32);
		dlqContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		dlqContainerProperties.setTopics(Set.of("dpmlct-016-dlq-topic"));
		DefaultPulsarMessageListenerContainer<Integer> dlqContainer = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, dlqContainerProperties);
		dlqContainer.start();

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<Integer>) (consumer, msg) -> {
			latch.countDown();
			if (msg.getValue() == 5) {
				throw new RuntimeException("fail");
			}
		});
		pulsarContainerProperties.setSchema(Schema.INT32);
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		DefaultPulsarMessageListenerContainer<Integer> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		DefaultPulsarProducerFactory<Integer> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-016");
		PulsarTemplate<Integer> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 1; i < 6; i++) {
			pulsarTemplate.send(i);
		}

		// DLQ consumer should receive 1 msg
		assertThat(dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();
		// Normal consumer should receive 5 msg + 1 re-delivery
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		dlqContainer.stop();
		pulsarClient.close();
	}

	@Test
	void deadLetterPolicyCustom() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
			.maxRedeliverCount(5)
			.deadLetterTopic("dlq-topic")
			.build();
		DefaultPulsarConsumerFactory<Integer> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic("dpmlct-017");
					consumerBuilder.subscriptionName("dpmlct-sb-017");
					consumerBuilder.negativeAckRedeliveryDelay(1L, TimeUnit.SECONDS);
					consumerBuilder.deadLetterPolicy(deadLetterPolicy);
				}));

		CountDownLatch dlqLatch = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(6);

		PulsarContainerProperties dlqContainerProperties = new PulsarContainerProperties();
		dlqContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> dlqLatch.countDown());
		dlqContainerProperties.setSchema(Schema.INT32);
		dlqContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		dlqContainerProperties.setTopics(Set.of("dlq-topic"));
		DefaultPulsarMessageListenerContainer<Integer> dlqContainer = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, dlqContainerProperties);
		dlqContainer.start();

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<Integer>) (consumer, msg) -> {
			latch.countDown();
			if (msg.getValue() == 5) {
				throw new RuntimeException("fail");
			}
		});
		pulsarContainerProperties.setSchema(Schema.INT32);
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		pulsarContainerProperties.getPulsarConsumerProperties()
			.put("deadLetterPolicy",
					DeadLetterPolicy.builder().maxRedeliverCount(1).deadLetterTopic("dlq-topic").build());

		DefaultPulsarMessageListenerContainer<Integer> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		DefaultPulsarProducerFactory<Integer> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"dpmlct-017");
		PulsarTemplate<Integer> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 1; i < 6; i++) {
			pulsarTemplate.send(i);
		}

		// DLQ consumer should receive 1 msg
		assertThat(dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();
		// Normal consumer should receive 5 msg + 1 re-delivery
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		dlqContainer.stop();
		pulsarClient.close();
	}

	@Test
	void batchListenerWithRecordAckModeNotSupported() {
		var containerProps = new PulsarContainerProperties();
		containerProps.setSchema(Schema.STRING);
		containerProps.transactions().setEnabled(true);
		containerProps.transactions().setTransactionManager(mock(PulsarAwareTransactionManager.class));
		containerProps.setBatchListener(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setMessageListener((PulsarBatchMessageListener<?>) (consumer, msgs) -> {
			throw new RuntimeException("should never get here");
		});
		var consumerFactory = new DefaultPulsarConsumerFactory<String>(mock(PulsarClient.class), List.of());
		var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
		assertThatIllegalStateException().isThrownBy(() -> container.start())
			.withCauseInstanceOf(IllegalStateException.class)
			.havingRootCause()
			.withMessage("Transactional batch listeners do not support AckMode.RECORD");
	}

	@Test
	void basicDefaultConsumerWithCustomObjectMapper() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var topic = "dpmlct-com-topic";
		var pulsarConsumerFactory = new DefaultPulsarConsumerFactory<UserRecord>(pulsarClient,
				List.of((consumerBuilder) -> {
					consumerBuilder.topic(topic);
					consumerBuilder.subscriptionName("dpmlct-com-sub");
				}));
		var latch = new CountDownLatch(1);
		AtomicReference<UserRecord> consumedRecordRef = new AtomicReference<>();
		var pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<UserRecord>) (consumer, msg) -> {
			consumedRecordRef.set(msg.getValue());
			latch.countDown();
		});

		// Prepare the schema with custom object mapper
		var objectMapper = UserRecordObjectMapper.withDeser();
		var schema = JSONSchemaUtil.schemaForTypeWithObjectMapper(UserRecord.class, objectMapper);
		pulsarContainerProperties.setSchema(schema);

		// Start the container
		var container = new DefaultPulsarMessageListenerContainer<>(pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		// Send and consume message and ensure the deser was used
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserRecord>(pulsarClient, topic);
		var pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		var sentUserRecord = new UserRecord("person", 51);
		// deser adds '-deser' to name and 5 to age
		var expectedReceivedUser = new UserRecord("person-deser", 56);
		pulsarTemplate.sendAsync(sentUserRecord);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(consumedRecordRef).hasValue(expectedReceivedUser);

		container.stop();
		pulsarClient.close();
	}

	private void safeStopContainer(PulsarMessageListenerContainer container) {
		try {
			container.stop();
		}
		catch (Exception ex) {
			logger.warn(ex, "Failed to stop container %s: %s".formatted(container, ex.getMessage()));
		}
	}

	@SuppressWarnings("unchecked")
	@Nested
	class WithStartupFailures {

		@Test
		void whenPolicyIsStopThenExceptionIsThrown() {
			DefaultPulsarConsumerFactory<String> consumerFactory = mock(DefaultPulsarConsumerFactory.class);
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.STOP);
			containerProps.setSchema(Schema.STRING);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex when create consumer
			var failCause = new PulsarException("please-stop");
			when(consumerFactory.createConsumer(any(Schema.class), any(), any(), any(), any())).thenThrow(failCause);
			// start container and expect ex thrown
			assertThatIllegalStateException().isThrownBy(() -> container.start())
				.withMessageStartingWith("Error starting listener container")
				.withCause(failCause);
			assertThat(container.isRunning()).isFalse();
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ConsumerFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsContinueThenExceptionIsNotThrown() {
			DefaultPulsarConsumerFactory<String> consumerFactory = mock(DefaultPulsarConsumerFactory.class);
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.CONTINUE);
			containerProps.setSchema(Schema.STRING);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex when create consumer
			var failCause = new PulsarException("please-continue");
			when(consumerFactory.createConsumer(any(Schema.class), any(), any(), any(), any())).thenThrow(failCause);
			// start container and expect ex not thrown
			container.start();
			assertThat(container.isRunning()).isFalse();
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ConsumerFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsRetryAndRetriesAreExhaustedThenContainerDoesNotStart() {
			DefaultPulsarConsumerFactory<String> consumerFactory = mock(DefaultPulsarConsumerFactory.class);
			var retryCount = new AtomicInteger(0);
			var thrown = new ArrayList<Throwable>();
			var retryListener = new RetryListener() {
				@Override
				public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					retryCount.set(context.getRetryCount());
				}

				@Override
				public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					thrown.add(throwable);
				}
			};
			var retryTemplate = RetryTemplate.builder()
				.maxAttempts(2)
				.fixedBackoff(Duration.ofSeconds(2))
				.withListener(retryListener)
				.build();
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			containerProps.setSchema(Schema.STRING);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex on 3 attempts (initial + 2 retries)
			var failCause = new PulsarException("please-retry-exhausted");
			doThrow(failCause).doThrow(failCause)
				.doThrow(failCause)
				.when(consumerFactory)
				.createConsumer(any(Schema.class), any(), any(), any(), any());
			container.start();

			// start container and expect ex not thrown and 2 retries
			await().atMost(Duration.ofSeconds(15)).until(() -> retryCount.get() == 2);
			assertThat(thrown).containsExactly(failCause, failCause);
			assertThat(container.isRunning()).isFalse();
			// factory called 3x (initial + 2 retries)
			verify(consumerFactory, times(3)).createConsumer(any(Schema.class), any(), any(), any(), any());
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ConsumerFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsRetryAndRetryIsSuccessfulThenContainerStarts() throws Exception {
			var topic = "dpmlct-wsf-retry";
			var pulsarClient = PulsarClient.builder()
				.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
			var consumerFactory = spy(
					new DefaultPulsarConsumerFactory<String>(pulsarClient, List.of((consumerBuilder) -> {
						consumerBuilder.topic(topic);
						consumerBuilder.subscriptionName(topic + "-sub");
					})));
			var retryCount = new AtomicInteger(0);
			var thrown = new ArrayList<Throwable>();
			var retryListener = new RetryListener() {
				@Override
				public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					retryCount.set(context.getRetryCount());
				}

				@Override
				public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					thrown.add(throwable);
				}
			};
			var retryTemplate = RetryTemplate.builder()
				.maxAttempts(3)
				.fixedBackoff(Duration.ofSeconds(2))
				.withListener(retryListener)
				.build();
			var latch = new CountDownLatch(1);
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
			containerProps.setSchema(Schema.STRING);
			var container = new DefaultPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex on initial call and 1st retry - then succeed on
			// 2nd retry
			var failCause = new PulsarException("please-retry");
			doThrow(failCause).doThrow(failCause)
				.doCallRealMethod()
				.when(consumerFactory)
				.createConsumer(any(Schema.class), any(), any(), any(), any());
			try {
				// start container and expect started after retries
				container.start();
				await().atMost(Duration.ofSeconds(10)).until(container::isRunning);

				// factory called 3x (initial call + 2 retries)
				verify(consumerFactory, times(3)).createConsumer(any(Schema.class), any(), any(), any(), any());
				// only had to retry once (2nd call in retry template succeeded)
				assertThat(retryCount).hasValue(1);
				assertThat(thrown).containsExactly(failCause);
				// should be able to process messages
				var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, topic);
				var pulsarTemplate = new PulsarTemplate<>(producerFactory);
				pulsarTemplate.sendAsync("hello-" + topic);
				assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			}
			finally {
				safeStopContainer(container);
			}
			pulsarClient.close();
		}

	}

}

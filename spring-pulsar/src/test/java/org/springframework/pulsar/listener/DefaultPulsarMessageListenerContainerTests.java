/*
 * Copyright 2022-2023 the original author or authors.
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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.ConsumerTestUtils;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 */
class DefaultPulsarMessageListenerContainerTests implements PulsarTestContainerSupport {

	@Test
	void basicDefaultConsumer() throws Exception {
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-012"), "subscriptionName",
				"dpmlct-sb-012");

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);
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
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("containerPauseResumeWaitNotify-topic"),
				"subscriptionName", "containerPauseResumeWaitNotify-sub");

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);
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

		Awaitility.await().until(container::isPaused);

		container.resume();

		Awaitility.await().until(() -> !container.isPaused());

		assertThat(latchOnLockInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnUnlockInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnAwaitInvocation.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latchOnSignalInvocation.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		pulsarClient.close();
	}

	@Test
	void subscriptionInitialPositionEarliest() throws Exception {
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-013"), "subscriptionName",
				"dpmlct-sb-013", "subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);
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
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-014"), "subscriptionName",
				"dpmlct-sb-014");

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);
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
		RedeliveryBackoff redeliveryBackoff = MultiplierRedeliveryBackoff.builder().minDelayMs(1000)
				.maxDelayMs(5 * 1000).build();
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-015"), "subscriptionName",
				"dpmlct-sb-015", "negativeAckRedeliveryBackoff", redeliveryBackoff);

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(
				new DefaultPulsarConsumerFactory<>(pulsarClient, config));
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
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder().maxRedeliverCount(1)
				.deadLetterTopic("dpmlct-016-dlq-topic").build();
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-016"), "subscriptionName",
				"dpmlct-sb-016", "negativeAckRedeliveryDelayMicros", TimeUnit.SECONDS.toMicros(1), "deadLetterPolicy",
				deadLetterPolicy);

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<Integer> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);

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
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder().maxRedeliverCount(5).deadLetterTopic("dlq-topic")
				.build();
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dpmlct-017"), "subscriptionName",
				"dpmlct-sb-016", "negativeAckRedeliveryDelayMicros", TimeUnit.SECONDS.toMicros(1), "deadLetterPolicy",
				deadLetterPolicy);

		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		DefaultPulsarConsumerFactory<Integer> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				config);

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
		pulsarContainerProperties.getPulsarConsumerProperties().put("deadLetterPolicy",
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

}

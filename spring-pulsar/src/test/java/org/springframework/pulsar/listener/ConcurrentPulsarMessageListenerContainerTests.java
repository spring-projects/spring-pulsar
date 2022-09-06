/*
 * Copyright 2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.PulsarConsumerFactory;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public class ConcurrentPulsarMessageListenerContainerTests {

	@Test
	@SuppressWarnings("unchecked")
	void deadLetterPolicyAppliedOnChildContainer() throws Exception {
		MockEnvironment env = setupMockEnvironment(SubscriptionType.Shared);
		ConcurrentPulsarMessageListenerContainer<String> concurrentContainer = env.concurrentContainer();
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder().maxRedeliverCount(5).deadLetterTopic("dlq-topic")
				.retryLetterTopic("retry-topic").build();
		concurrentContainer.setDeadLetterPolicy(deadLetterPolicy);

		concurrentContainer.start();

		final DefaultPulsarMessageListenerContainer<String> childContainer = concurrentContainer.getContainers().get(0);
		assertThat(childContainer.getDeadLetterPolicy()).isEqualTo(deadLetterPolicy);
	}

	@Test
	@SuppressWarnings("unchecked")
	void nackRedeliveryBackoffAppliedOnChildContainer() throws Exception {
		MockEnvironment env = setupMockEnvironment(SubscriptionType.Shared);
		ConcurrentPulsarMessageListenerContainer<String> concurrentContainer = env.concurrentContainer();
		RedeliveryBackoff redeliveryBackoff = MultiplierRedeliveryBackoff.builder().minDelayMs(1000)
				.maxDelayMs(5 * 1000).build();
		concurrentContainer.setNegativeAckRedeliveryBackoff(redeliveryBackoff);

		concurrentContainer.start();

		final DefaultPulsarMessageListenerContainer<String> childContainer = concurrentContainer.getContainers().get(0);
		assertThat(childContainer.getNegativeAckRedeliveryBackoff()).isEqualTo(redeliveryBackoff);
	}

	@Test
	@SuppressWarnings("unchecked")
	void basicConcurrencyTesting() throws Exception {
		MockEnvironment env = setupMockEnvironment(SubscriptionType.Failover);
		PulsarConsumerFactory<String> pulsarConsumerFactory = env.consumerFactory();
		Consumer<String> consumer = env.consumer();
		ConcurrentPulsarMessageListenerContainer<String> concurrentContainer = env.concurrentContainer();

		concurrentContainer.setConcurrency(3);

		concurrentContainer.start();

		await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(pulsarConsumerFactory, times(3))
				.createConsumer(any(Schema.class), any(BatchReceivePolicy.class), any(Map.class)));
		await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(consumer, times(3)).batchReceive());
	}

	@Test
	@SuppressWarnings("unchecked")
	void exclusiveSubscriptionMustUseSingleThread() throws Exception {
		MockEnvironment env = setupMockEnvironment(SubscriptionType.Exclusive);
		ConcurrentPulsarMessageListenerContainer<String> concurrentContainer = env.concurrentContainer();

		concurrentContainer.setConcurrency(3);

		assertThatThrownBy(concurrentContainer::start).isInstanceOf(IllegalStateException.class)
				.hasMessage("concurrency > 1 is not allowed on Exclusive subscription type");
	}

	@SuppressWarnings("unchecked")
	private MockEnvironment setupMockEnvironment(SubscriptionType subscriptionType) throws Exception {
		PulsarConsumerFactory<String> pulsarConsumerFactory = mock(PulsarConsumerFactory.class);
		Consumer<String> consumer = mock(Consumer.class);

		when(pulsarConsumerFactory.createConsumer(any(Schema.class), any(BatchReceivePolicy.class), any(Map.class)))
				.thenReturn(consumer);

		when(consumer.batchReceive()).thenReturn(mock(Messages.class));

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setSubscriptionType(subscriptionType);
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (cons, msg) -> {
		});

		ConcurrentPulsarMessageListenerContainer<String> concurrentContainer = new ConcurrentPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);

		return new MockEnvironment(pulsarConsumerFactory, consumer, concurrentContainer);
	}

	private record MockEnvironment(PulsarConsumerFactory<String> consumerFactory, Consumer<String> consumer,
			ConcurrentPulsarMessageListenerContainer<String> concurrentContainer) {
	}

}

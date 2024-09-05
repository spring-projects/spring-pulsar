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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.retry.support.RetryTemplate;

/**
 * Tests the startup failures policy on the
 * {@link ConcurrentPulsarMessageListenerContainer}.
 */
@SuppressWarnings("unchecked")
class ConcurrentPulsarMessageListenerContainerStartupFailureTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void whenPolicyIsStopThenExceptionIsThrown() throws Exception {
		var topic = "cpmlcsft-stop";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ConcurrentPulsarMessageListenerContainer<String> container = null;
		try {
			var consumerFactory = spy(
					new DefaultPulsarConsumerFactory<String>(pulsarClient, List.of((consumerBuilder) -> {
						consumerBuilder.topic(topic);
						consumerBuilder.subscriptionName(topic + "-sub");
						consumerBuilder.subscriptionType(SubscriptionType.Shared);
					})));
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.STOP);
			containerProps.setSchema(Schema.STRING);
			containerProps.setConcurrency(3);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (__, ___) -> {
			});
			container = new ConcurrentPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			container.setConcurrency(containerProps.getConcurrency());

			// setup factory (c1 pass, c2 fail, c3 pass)
			var failCause = new IllegalStateException("please-stop");
			doCallRealMethod().doThrow(failCause)
				.doCallRealMethod()
				.when(consumerFactory)
				.createConsumer(any(Schema.class), any(), any(), any(), any());

			var parentContainer = container;
			assertThatIllegalStateException().isThrownBy(() -> parentContainer.start())
				.withMessageStartingWith("Error starting listener container [consumer-1]")
				.withCause(failCause);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenPolicyIsContinueThenExceptionIsNotThrown() throws Exception {
		var topic = "cpmlcsft-continue";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ConcurrentPulsarMessageListenerContainer<String> container = null;
		try {
			var consumerFactory = spy(
					new DefaultPulsarConsumerFactory<String>(pulsarClient, List.of((consumerBuilder) -> {
						consumerBuilder.topic(topic);
						consumerBuilder.subscriptionName(topic + "-sub");
						consumerBuilder.subscriptionType(SubscriptionType.Shared);
					})));
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.CONTINUE);
			containerProps.setSchema(Schema.STRING);
			containerProps.setConcurrency(3);
			var latch = new CountDownLatch(1);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
			container = new ConcurrentPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			container.setConcurrency(containerProps.getConcurrency());

			// setup factory (c1 pass, c2 fail, c3 pass)
			var failCause = new IllegalStateException("please-continue");
			doCallRealMethod().doThrow(failCause)
				.doCallRealMethod()
				.when(consumerFactory)
				.createConsumer(any(Schema.class), any(), any(), any(), any());

			// start container and expect started after retries
			container.start();
			assertThat(container.getContainers()).hasSize(3)
				.extracting(DefaultPulsarMessageListenerContainer::isRunning)
				.containsExactly(true, false, true);
			assertThat(container.isRunning()).isTrue();

			// should be able to process messages
			var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, topic);
			var pulsarTemplate = new PulsarTemplate<>(producerFactory);
			pulsarTemplate.sendAsync("hello-" + topic);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void whenPolicyIsRetryAndRetryIsSuccessfulThenContainerStarts() throws Exception {
		var topic = "cpmlcsft-retry";
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ConcurrentPulsarMessageListenerContainer<String> container = null;
		try {
			var consumerFactory = spy(
					new DefaultPulsarConsumerFactory<String>(pulsarClient, List.of((consumerBuilder) -> {
						consumerBuilder.topic(topic);
						consumerBuilder.subscriptionName(topic + "-sub");
						consumerBuilder.subscriptionType(SubscriptionType.Shared);
					})));
			var containerProps = new PulsarContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setSchema(Schema.STRING);
			containerProps.setConcurrency(3);
			var retryTemplate = RetryTemplate.builder().maxAttempts(2).fixedBackoff(Duration.ofSeconds(2)).build();
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			var latch = new CountDownLatch(1);
			containerProps.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
			container = new ConcurrentPulsarMessageListenerContainer<>(consumerFactory, containerProps);
			container.setConcurrency(containerProps.getConcurrency());

			// setup factory (c1 fail, c2 fail/retry, c3 fail)
			var failCause = new IllegalStateException("please-retry");
			doThrow(failCause).doThrow(failCause)
				.doThrow(failCause)
				.doCallRealMethod()
				.doCallRealMethod()
				.doCallRealMethod()
				.when(consumerFactory)
				.createConsumer(any(Schema.class), any(), any(), any(), any());

			// start container and expect started after retries
			container.start();
			var parentContainer = container;
			await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(parentContainer.getContainers()).hasSize(3)
					.extracting(DefaultPulsarMessageListenerContainer::isRunning)
					.containsExactly(true, true, true));
			assertThat(container.isRunning()).isTrue();

			// should be able to process messages
			var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, topic);
			var pulsarTemplate = new PulsarTemplate<>(producerFactory);
			pulsarTemplate.sendAsync("hello-" + topic);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
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

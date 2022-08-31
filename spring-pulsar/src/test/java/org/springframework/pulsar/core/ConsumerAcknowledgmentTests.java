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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarAcknowledgingMessageListener;
import org.springframework.pulsar.listener.PulsarBatchMessageListener;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;
import org.springframework.util.Assert;

/**
 * @author Soby Chacko
 */
class ConsumerAcknowledgmentTests extends AbstractContainerBaseTests {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	static int COUNTER = 0;

	@Test
	void testRecordAck() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-011");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-011");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setAckMode(PulsarContainerProperties.AckMode.RECORD);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		CountDownLatch latch = new CountDownLatch(10);

		doAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).when(containerConsumer).acknowledge(any(Message.class));

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-011");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Test
	void testBatchAck() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-012");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-012");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);
		pulsarContainerProperties
				.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-012");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, never()).acknowledge(any(Message.class)));
		await().atMost(Duration.ofSeconds(10)).untilAsserted(
				() -> verify(containerConsumer, atLeastOnce()).acknowledgeCumulative(any(Message.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	void testBatchAckButSomeRecordsFail() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-013");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-013");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);

		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			latch.countDown();
			this.logger.warn("Listener invoked: " + COUNTER++);
			this.logger.warn("Msg received: " + msg.getValue());
			if (latch.getCount() % 2 == 0) {
				this.logger.error("Failing to acknowledge.");
				throw new RuntimeException("fail");
			}
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-013");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(1_000);
		// Half of the message get acknowledged, and the other half gets negatively
		// acknowledged.
		try {
			await().atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> verify(containerConsumer, times(5)).acknowledge(any(Message.class)));
		}
		catch (Throwable t) {
			this.logger.error(t.getMessage());
		}
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, times(5)).negativeAcknowledge(any(Message.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testManualAckForRecordListener() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-014");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-014");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		final List<Acknowledgement> acksObjects = new ArrayList<>();
		PulsarAcknowledgingMessageListener<?> pulsarAcknowledgingMessageListener = (consumer, msg, acknowledgement) -> {
			acksObjects.add(acknowledgement);
			acknowledgement.acknowledge();
		};

		pulsarContainerProperties.setMessageListener(pulsarAcknowledgingMessageListener);

		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setAckMode(PulsarContainerProperties.AckMode.MANUAL);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		CountDownLatch latch = new CountDownLatch(10);

		doAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).when(containerConsumer).acknowledge(any(Message.class));

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-014");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		// We are asserting that we got 10 valid ack objects through the receive method
		// invocation.
		assertThat(acksObjects.size()).isEqualTo(10);
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, times(10)).acknowledge(any(Message.class)));

		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBatchAckForBatchListener() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-015");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-015");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMaxNumMessages(10);
		pulsarContainerProperties.setBatchTimeout(60_000);
		pulsarContainerProperties.setBatchListener(true);
		CountDownLatch latch = new CountDownLatch(1);
		final PulsarBatchMessageListener<?> pulsarBatchMessageListener = mock(PulsarBatchMessageListener.class);

		doAnswer(invocation -> {
			latch.countDown();
			return null;
		}).when(pulsarBatchMessageListener).received(any(Consumer.class), any(Messages.class));

		pulsarContainerProperties.setMessageListener(pulsarBatchMessageListener);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-015");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		await().atMost(Duration.ofSeconds(10)).untilAsserted(
				() -> verify(pulsarBatchMessageListener, times(1)).received(any(Consumer.class), any(Messages.class)));
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, times(1)).acknowledgeCumulative(any(Message.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBatchNackForEntireBatchWhenUsingBatchListener() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("cons-ack-tests-016");
		config.put("topicNames", strings);
		config.put("subscriptionName", "cons-ack-tests-sb-016");
		final PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMaxNumMessages(10);
		pulsarContainerProperties.setBatchTimeout(60_000);
		pulsarContainerProperties.setBatchListener(true);
		final PulsarBatchMessageListener<?> pulsarBatchMessageListener = mock(PulsarBatchMessageListener.class);
		CountDownLatch latch = new CountDownLatch(1);

		doAnswer(invocation -> {
			latch.countDown();
			throw new RuntimeException();
		}).when(pulsarBatchMessageListener).received(any(Consumer.class), any(Messages.class));

		pulsarContainerProperties.setMessageListener(pulsarBatchMessageListener);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "cons-ack-tests-016");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(2_000);
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, times(1)).negativeAcknowledge(any(Messages.class)));
		await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, never()).acknowledge(any(Messages.class)));
		container.stop();
		pulsarClient.close();
	}

	private Consumer<?> spyOnConsumer(DefaultPulsarMessageListenerContainer<String> container) {
		Consumer<?> consumer = getPropertyValue(container, "listenerConsumer.consumer", Consumer.class);
		consumer = spy(consumer);
		new DirectFieldAccessor(getPropertyValue(container, "listenerConsumer")).setPropertyValue("consumer", consumer);
		return consumer;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to obtain a property using dotted notation
	 * to traverse fields; e.g. "foo.bar.baz" will obtain a reference to the baz field of
	 * the bar field of foo. Adopted from Spring Integration.
	 * @param root The object.
	 * @param propertyPath The path.
	 * @return The field.
	 */
	public static Object getPropertyValue(Object root, String propertyPath) {
		Object value = null;
		DirectFieldAccessor accessor = new DirectFieldAccessor(root);
		String[] tokens = propertyPath.split("\\.");
		for (int i = 0; i < tokens.length; i++) {
			value = accessor.getPropertyValue(tokens[i]);
			if (value != null) {
				accessor = new DirectFieldAccessor(value);
			}
			else if (i == tokens.length - 1) {
				return null;
			}
			else {
				throw new IllegalArgumentException("intermediate property '" + tokens[i] + "' is null");
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(Object root, String propertyPath, Class<T> type) {
		Object value = getPropertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(type, value.getClass());
		}
		return (T) value;
	}

}

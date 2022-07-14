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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarAcknowledgingMessageListener;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;
import org.springframework.util.Assert;

/**
 * @author Soby Chacko
 */
class PulsarMessageListenerContainerTests extends AbstractContainerBaseTests {

	@Test
	void testRecordAck() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("foobar-013");
		config.put("topicNames", strings);
		config.put("subscriptionName", "foobar-sb-013");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(getPulsarBrokerUrl())
				.build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener(
				(PulsarRecordMessageListener<?>) (consumer, msg) -> {
				});
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setAckMode(PulsarContainerProperties.AckMode.RECORD);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		CountDownLatch latch = new CountDownLatch(10);

		willAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).given(containerConsumer)
				.acknowledge(any(Message.class));

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "foobar-013");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, prodConfig);
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
		strings.add("foobar-014");
		config.put("topicNames", strings);
		config.put("subscriptionName", "foobar-sb-014");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(getPulsarBrokerUrl())
				.build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);
		pulsarContainerProperties.setMessageListener(
				(PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "foobar-014");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		verify(containerConsumer, never()).acknowledge(any(Message.class));
		verify(containerConsumer, atLeastOnce()).acknowledge(any(Messages.class));
		container.stop();
		pulsarClient.close();
	}

	@Test
	void testBatchAckButSomeRecordsFail() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("foobar-014");
		config.put("topicNames", strings);
		config.put("subscriptionName", "foobar-sb-014");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(getPulsarBrokerUrl())
				.build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);
		pulsarContainerProperties.setMessageListener(
				(PulsarRecordMessageListener<?>) (consumer, msg) -> {
					latch.countDown();
					if (latch.getCount() % 2 == 0) {
						throw new RuntimeException("fail");
					}
				}
		);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		final Consumer<?> containerConsumer = spyOnConsumer(container);

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "foobar-014");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		// Half of the message get acknowledged, and the other half gets nacked.
		verify(containerConsumer, times(5)).acknowledge(any(Message.class));
		verify(containerConsumer, times(5)).negativeAcknowledge(any(Message.class));
		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testManualAckForRecordListener() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final Set<String> strings = new HashSet<>();
		strings.add("foobar-013");
		config.put("topicNames", strings);
		config.put("subscriptionName", "foobar-sb-013");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(getPulsarBrokerUrl())
				.build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);

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

		willAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).given(containerConsumer)
				.acknowledge(any(Message.class));

		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "foobar-013");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		//We are asserting that we got 10 valid ack objects through the receive method invocation.
		assertThat(acksObjects.size()).isEqualTo(10);
		verify(containerConsumer, times(10)).acknowledge(any(Message.class));

		container.stop();
		pulsarClient.close();
	}

	private Consumer<?> spyOnConsumer(DefaultPulsarMessageListenerContainer<String> container) {
		Consumer<?> consumer = getPropertyValue(container, "listenerConsumer.consumer", Consumer.class);
		consumer = spy(consumer);
		new DirectFieldAccessor(getPropertyValue(container, "listenerConsumer"))
				.setPropertyValue("consumer", consumer);
		return consumer;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to obtain a property using dotted notation to traverse fields; e.g.
	 * "foo.bar.baz" will obtain a reference to the baz field of the bar field of foo. Adopted from Spring Integration.
	 *
	 * @param root         The object.
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

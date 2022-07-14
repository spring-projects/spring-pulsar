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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;

/**
 * @author Soby Chacko
 */
class DefaultConsumerTests extends AbstractContainerBaseTests {

	@Test
	void testDefaultConsumer() throws Exception {
		Map<String, Object> config = new HashMap<>();
		final HashSet<String> strings = new HashSet<String>();
		strings.add("foobar-012");
		config.put("topicNames", strings);
		config.put("subscriptionName", "foobar-sb-012");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(getPulsarBrokerUrl())
				.build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);
		CountDownLatch latch = new CountDownLatch(1);
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener(
				(PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "foobar-012");
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		final CompletableFuture<MessageId> future = pulsarTemplate.sendAsync("hello john doe");
		latch.await(10, TimeUnit.SECONDS);
		pulsarClient.close();
	}
}

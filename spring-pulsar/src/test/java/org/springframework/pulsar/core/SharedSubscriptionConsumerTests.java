/*
 * Copyright 2023 the original author or authors.
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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Tests for shared subscription types in Pulsar consumer.
 *
 * @author Soby Chacko
 */
public class SharedSubscriptionConsumerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void keySharedSubscriptionWithDefaultAutoSplitHashingRange() throws Exception {
		DefaultPulsarMessageListenerContainer<String> container1 = null;
		DefaultPulsarMessageListenerContainer<String> container2 = null;
		DefaultPulsarMessageListenerContainer<String> container3 = null;
		PulsarClient pulsarClient = null;
		try {
			pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
			DefaultPulsarConsumerFactory<String> consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
					Map.of("topicNames", Collections.singleton("key-shared-batch-disabled-topic"), "subscriptionName",
							"key-shared-batch-disabled-sub"));

			CountDownLatch latch1 = new CountDownLatch(10);
			CountDownLatch latch2 = new CountDownLatch(10);
			CountDownLatch latch3 = new CountDownLatch(10);
			container1 = createAndStartContainer(consumerFactory, latch1, "hello alice doe");
			container2 = createAndStartContainer(consumerFactory, latch2, "hello buzz doe");
			container3 = createAndStartContainer(consumerFactory, latch3, "hello john doe");
			logger.info("**** Containers all started - pausing 10s");
			Thread.sleep(10_000);

			DefaultPulsarProducerFactory<String> producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Map.of("topicName", "key-shared-batch-disabled-topic", "batchingEnabled", "false"));
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory);
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.newMessage("hello alice doe")
						.withMessageCustomizer(messageBuilder -> messageBuilder.key("alice")).send();
				pulsarTemplate.newMessage("hello buzz doe")
						.withMessageCustomizer(messageBuilder -> messageBuilder.key("buzz")).send();
				pulsarTemplate.newMessage("hello john doe")
						.withMessageCustomizer(messageBuilder -> messageBuilder.key("john")).send();
			}
			logger.info("**** Sent all messages");

			Awaitility.await().atMost(Duration.ofSeconds(30)).pollDelay(Duration.ofSeconds(5)).until(() -> {
				long count1 = latch1.getCount();
				long count2 = latch2.getCount();
				long count3 = latch3.getCount();
				logger.info("**** L1 %d L2 %d L3 %d".formatted(count1, count2, count3));
				return count1 == 0 && count2 == 0 && count3 == 0;
			});
		}
		finally {
			safeStopContainer(container1);
			safeStopContainer(container2);
			safeStopContainer(container3);
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

	private DefaultPulsarMessageListenerContainer<String> createAndStartContainer(
			PulsarConsumerFactory<String> consumerFactory, CountDownLatch latch, String expectedMessage) {
		PulsarContainerProperties containerProps = pulsarContainerProperties(latch, expectedMessage,
				SubscriptionType.Key_Shared);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				consumerFactory, containerProps);
		container.start();
		return container;
	}

	private PulsarContainerProperties pulsarContainerProperties(CountDownLatch latch, String message,
			SubscriptionType subscriptionType) {
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setBatchListener(false);
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			logger.info("CONTAINER(%s) got: %s".formatted(message, msg.getValue()));
			if (msg.getValue().equals(message)) {
				latch.countDown();
			}
		});
		pulsarContainerProperties.setSubscriptionType(subscriptionType);
		pulsarContainerProperties.setSchema(Schema.STRING);
		return pulsarContainerProperties;
	}

}

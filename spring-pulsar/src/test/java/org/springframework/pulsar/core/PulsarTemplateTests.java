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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

/**
 * @author Soby Chacko
 */
class PulsarTemplateTests extends AbstractContainerBaseTests {

	public static final String TEST_TOPIC = "test_topic";

	@Test
	void testUsage() throws Exception {
		testPulsarFunctionality(getPulsarBrokerUrl());
	}

	@Test
	void testSendAsync() throws Exception {
		Map<String, Object> config = new HashMap<>();
		config.put("topicName", "foo-bar-123");
		Map<String, Object> clientConfig = new HashMap<>();
		clientConfig.put("serviceUrl", getPulsarBrokerUrl());
		try (
				PulsarClient client = PulsarClient.builder()
						.loadConf(clientConfig)
						.build();
				Consumer<String> consumer = client.newConsumer(Schema.STRING)
						.topic("foo-bar-123")
						.subscriptionName("xyz-test-subs-123")
						.subscribe()
		) {
			final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(client, config);
			final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			final CompletableFuture<MessageId> future = pulsarTemplate.sendAsync("hello john doe");
			future.thenAccept(m -> { });
			try {
				Thread.sleep(2000);
				future.get();
			}
			catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			CompletableFuture<Message<String>> future0 = consumer.receiveAsync();
			Message<String> message = future0.get(5, TimeUnit.SECONDS);
			assertThat(new String(message.getData()))
					.isEqualTo("hello john doe");
		}
	}

	@Test
	void testSendSync() throws Exception {
		Map<String, Object> config = new HashMap<>();
		config.put("topicName", "foo-bar-123");
		Map<String, Object> clientConfig = new HashMap<>();
		clientConfig.put("serviceUrl", getPulsarBrokerUrl());
		try (
				PulsarClient client = PulsarClient.builder()
						.loadConf(clientConfig)
						.build();
				Consumer<String> consumer = client.newConsumer(Schema.STRING)
						.topic("foo-bar-123")
						.subscriptionName("xyz-test-subs-123")
						.subscribe();
		) {
			final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(client, config);
			final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			final MessageId messageId = pulsarTemplate.send("hello john doe");
			CompletableFuture<Message<String>> future0 = consumer.receiveAsync();
			Message<String> message = future0.get(5, TimeUnit.SECONDS);
			assertThat(new String(message.getData()))
					.isEqualTo("hello john doe");
		}
	}

	private void testPulsarFunctionality(String pulsarBrokerUrl) throws Exception {
		try (
				PulsarClient client = PulsarClient.builder()
						.serviceUrl(pulsarBrokerUrl)
						.build();
				Consumer<byte[]> consumer = client.newConsumer()
						.topic(TEST_TOPIC)
						.subscriptionName("test-subs")
						.subscribe();
				Producer<byte[]> producer = client.newProducer()
						.topic(TEST_TOPIC)
						.create()
		) {
			producer.send("test containers".getBytes());
			CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
			Message<byte[]> message = future.get(5, TimeUnit.SECONDS);

			assertThat(new String(message.getData()))
					.isEqualTo("test containers");
		}
	}
}

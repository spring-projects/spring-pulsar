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

import java.io.Serial;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMetadata;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;

/**
 * @author Soby Chacko
 */
class FailoverConsumerTests implements PulsarTestContainerSupport {

	@Test
	void testFailOverConsumersOnPartitionedTopic() throws Exception {
		PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl())
				.build();

		String topicName = "persistent://public/default/my-part-topic-1";
		int numPartitions = 3;
		admin.topics().createPartitionedTopic(topicName, numPartitions);

		Map<String, Object> config = new HashMap<>();
		final HashSet<String> topics = new HashSet<>();
		topics.add("my-part-topic-1");
		config.put("topicNames", topics);
		config.put("subscriptionName", "my-part-subscription-1");
		final PulsarClient pulsarClient = PulsarClient.builder()
				.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		final DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, config);
		CountDownLatch latch = new CountDownLatch(3);
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties
				.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Failover);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		DefaultPulsarMessageListenerContainer<String> container1 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container1.start();
		DefaultPulsarMessageListenerContainer<String> container2 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container2.start();
		Map<String, Object> prodConfig = new HashMap<>();
		prodConfig.put("topicName", "my-part-topic-1");
		prodConfig.put("messageRoutingMode", MessageRoutingMode.CustomPartition);
		final DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
				pulsarClient, prodConfig);
		final PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);

		pulsarTemplate.newMessage("hello john doe").withCustomRouter(new FooRouter()).sendAsync();
		pulsarTemplate.newMessage("hello alice doe").withCustomRouter(new BarRouter()).sendAsync();
		pulsarTemplate.newMessage("hello buzz doe").withCustomRouter(new BuzzRouter()).sendAsync();

		final boolean await = latch.await(10, TimeUnit.SECONDS);
		assertThat(await).isTrue();
	}

	static class FooRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 0;
		}

	}

	static class BarRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 1;
		}

	}

	static class BuzzRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 2;
		}

	}

}

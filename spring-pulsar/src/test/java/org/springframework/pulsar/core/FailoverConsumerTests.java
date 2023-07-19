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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serial;
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
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * @author Soby Chacko
 */
class FailoverConsumerTests implements PulsarTestContainerSupport {

	@Test
	void testFailOverConsumersOnPartitionedTopic() throws Exception {
		PulsarAdmin admin = PulsarAdmin.builder()
			.serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl())
			.build();
		String topicName = "persistent://public/default/my-part-topic-1";
		admin.topics().createPartitionedTopic(topicName, 3);

		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				(consumerBuilder) -> {
					consumerBuilder.topic("my-part-topic-1");
					consumerBuilder.subscriptionName("my-part-subscription-1");
				});

		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);
		CountDownLatch latch3 = new CountDownLatch(1);

		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch1.countDown());
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Failover);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container1 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container1.start();

		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch2.countDown());
		DefaultPulsarMessageListenerContainer<String> container2 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container2.start();

		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch3.countDown());
		DefaultPulsarMessageListenerContainer<String> container3 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container3.start();

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"my-part-topic-1", (pb) -> pb.messageRoutingMode(MessageRoutingMode.CustomPartition));
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);

		pulsarTemplate.newMessage("hello john doe")
			.withProducerCustomizer(builder -> builder.messageRouter(new FooRouter()))
			.sendAsync();
		pulsarTemplate.newMessage("hello alice doe")
			.withProducerCustomizer(builder -> builder.messageRouter(new BarRouter()))
			.sendAsync();
		pulsarTemplate.newMessage("hello buzz doe")
			.withProducerCustomizer(builder -> builder.messageRouter(new BuzzRouter()))
			.sendAsync();

		boolean await1 = latch1.await(10, TimeUnit.SECONDS);
		boolean await2 = latch2.await(10, TimeUnit.SECONDS);
		boolean await3 = latch3.await(10, TimeUnit.SECONDS);

		assertThat(await1).isTrue();
		assertThat(await2).isTrue();
		assertThat(await3).isTrue();
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

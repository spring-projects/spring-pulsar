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

package org.springframework.pulsar.listener.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.PulsarTestContainerSupport;
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderTemplate;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for {@link DefaultReactivePulsarMessageListenerContainer}
 *
 * @author Christophe Bornet
 */
class DefaultReactivePulsarMessageListenerContainerTests implements PulsarTestContainerSupport {

	@Test
	void messageHandlerListener() throws Exception {
		String topic = "drpmlct-012";
		MutableReactiveMessageConsumerSpec config = new MutableReactiveMessageConsumerSpec();
		config.setTopicNames(Collections.singletonList(topic));
		config.setSubscriptionName("drpmlct-sb-012");
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, config);
		// Ensure subscription is created
		pulsarConsumerFactory.createConsumer(Schema.STRING).consumeNothing().block(Duration.ofSeconds(10));
		CountDownLatch latch = new CountDownLatch(1);
		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties
				.setMessageHandler((ReactivePulsarMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		MutableReactiveMessageSenderSpec prodConfig = new MutableReactiveMessageSenderSpec();
		prodConfig.setTopicName(topic);
		DefaultReactivePulsarSenderFactory<String> pulsarProducerFactory = new DefaultReactivePulsarSenderFactory<>(
				reactivePulsarClient, prodConfig, null);
		ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(pulsarProducerFactory);
		pulsarTemplate.send("hello john doe").subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Test
	void streamingHandlerListener() throws Exception {
		String topic = "drpmlct-013";
		MutableReactiveMessageConsumerSpec config = new MutableReactiveMessageConsumerSpec();
		config.setTopicNames(Collections.singletonList(topic));
		config.setSubscriptionName("drpmlct-sb-013");
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, config);
		// Ensure subscription is created
		pulsarConsumerFactory.createConsumer(Schema.STRING).consumeNothing().block(Duration.ofSeconds(10));
		CountDownLatch latch = new CountDownLatch(5);
		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties.setMessageHandler((ReactivePulsarStreamingHandler<String>) (msg) -> msg.map(m -> {
			latch.countDown();
			return MessageResult.acknowledge(m.getMessageId());
		}));
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		MutableReactiveMessageSenderSpec prodConfig = new MutableReactiveMessageSenderSpec();
		prodConfig.setTopicName(topic);
		DefaultReactivePulsarSenderFactory<String> pulsarProducerFactory = new DefaultReactivePulsarSenderFactory<>(
				reactivePulsarClient, prodConfig, null);
		ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(pulsarProducerFactory);
		Flux.range(0, 5).map(i -> "hello john doe" + i).as(pulsarTemplate::send).subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Test
	void containerProperties() throws Exception {
		String topic = "drpmlct-sb-014";
		String subscriptionName = "drpmlct-sb-014";
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, null);
		// Ensure subscription is created
		pulsarConsumerFactory
				.createConsumer(Schema.STRING,
						Collections.singletonList(
								c -> c.topicNames(Collections.singletonList(topic)).subscriptionName(subscriptionName)))
				.consumeNothing().block(Duration.ofSeconds(10));
		CountDownLatch latch = new CountDownLatch(1);
		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties
				.setMessageHandler((ReactivePulsarMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setTopics(new String[] { topic });
		pulsarContainerProperties.setSubscriptionName(subscriptionName);
		pulsarContainerProperties.setConcurrency(5);
		pulsarContainerProperties.setMaxInFlight(6);
		pulsarContainerProperties.setHandlingTimeoutMillis(7);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		MutableReactiveMessageSenderSpec prodConfig = new MutableReactiveMessageSenderSpec();
		prodConfig.setTopicName(topic);
		DefaultReactivePulsarSenderFactory<String> pulsarProducerFactory = new DefaultReactivePulsarSenderFactory<>(
				reactivePulsarClient, prodConfig, null);
		ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(pulsarProducerFactory);
		pulsarTemplate.send("hello john doe").subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(container).extracting("pipeline", InstanceOfAssertFactories.type(ReactiveMessagePipeline.class))
				.hasFieldOrPropertyWithValue("concurrency", 5).hasFieldOrPropertyWithValue("maxInflight", 6)
				.hasFieldOrPropertyWithValue("handlingTimeout", Duration.ofMillis(7));

		container.stop();
		pulsarClient.close();
	}

	@Test
	void defaultSubscriptionType() throws Exception {
		String topic = "drpmlct-015";
		MutableReactiveMessageConsumerSpec config = new MutableReactiveMessageConsumerSpec();
		config.setTopicNames(Collections.singletonList(topic));
		config.setSubscriptionName("drpmlct-sb-015");
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, config);

		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties.setMessageHandler((ReactivePulsarMessageHandler<String>) (msg) -> Mono.empty());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		Thread.sleep(2_000);

		StepVerifier
				.create(pulsarConsumerFactory
						.createConsumer(Schema.STRING,
								Collections.singletonList(c -> c.subscriptionType(SubscriptionType.Shared)))
						.consumeNothing())
				.expectError().verify(Duration.ofSeconds(10));

		container.stop();
		pulsarClient.close();
	}

	@Test
	void containerSubscriptionType() throws Exception {
		String topic = "drpmlct-016";
		MutableReactiveMessageConsumerSpec config = new MutableReactiveMessageConsumerSpec();
		config.setTopicNames(Collections.singletonList(topic));
		config.setSubscriptionName("drpmlct-sb-016");
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, config);

		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties.setMessageHandler((ReactivePulsarMessageHandler<String>) (msg) -> Mono.empty());
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();

		Thread.sleep(2_000);

		StepVerifier
				.create(pulsarConsumerFactory
						.createConsumer(Schema.STRING,
								Collections.singletonList(c -> c.subscriptionType(SubscriptionType.Shared)))
						.consumeNothing())
				.expectComplete().verify(Duration.ofSeconds(10));

		container.stop();
		pulsarClient.close();
	}

	@Test
	void containerTopicsPattern() throws Exception {
		String topic = "drpmlct-017-foo";
		String subscriptionName = "drpmlct-sb-017";
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, null);
		// Ensure subscription is created
		pulsarConsumerFactory
				.createConsumer(Schema.STRING,
						Collections.singletonList(
								c -> c.topicNames(Collections.singletonList(topic)).subscriptionName(subscriptionName)))
				.consumeNothing().block(Duration.ofSeconds(10));
		CountDownLatch latch = new CountDownLatch(1);
		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties
				.setMessageHandler((ReactivePulsarMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setTopicsPattern("persistent://public/default/drpmlct-017-.*");
		pulsarContainerProperties.setSubscriptionName(subscriptionName);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container.start();
		MutableReactiveMessageSenderSpec prodConfig = new MutableReactiveMessageSenderSpec();
		prodConfig.setTopicName(topic);
		DefaultReactivePulsarSenderFactory<String> pulsarProducerFactory = new DefaultReactivePulsarSenderFactory<>(
				reactivePulsarClient, prodConfig, null);
		ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(pulsarProducerFactory);
		pulsarTemplate.send("hello john doe").subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		pulsarClient.close();
	}

	@Test
	void consumerCustomizer() throws Exception {
		String topic = "drpmlct-018";
		String deadLetterTopic = "drpmlct-018-dlq-topic";
		MutableReactiveMessageConsumerSpec config = new MutableReactiveMessageConsumerSpec();
		config.setTopicNames(Collections.singletonList(topic));
		config.setSubscriptionName("drpmlct-sb-018");
		config.setNegativeAckRedeliveryDelay(Duration.ZERO);
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
		ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
		DefaultReactivePulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				reactivePulsarClient, config);
		ReactiveMessageConsumer<String> dlqConsumer = pulsarConsumerFactory.createConsumer(Schema.STRING,
				Collections.singletonList(b -> b.topicNames(Collections.singletonList(deadLetterTopic))));

		// Ensure subscriptions are created
		pulsarConsumerFactory.createConsumer(Schema.STRING).consumeNothing().block(Duration.ofSeconds(10));
		dlqConsumer.consumeNothing().block(Duration.ofSeconds(10));

		CountDownLatch latch = new CountDownLatch(6);
		ReactivePulsarContainerProperties pulsarContainerProperties = new ReactivePulsarContainerProperties();
		pulsarContainerProperties.setMessageHandler((ReactivePulsarStreamingHandler<String>) (msg) -> msg.map(m -> {
			latch.countDown();
			if (m.getValue().endsWith("4")) {
				return MessageResult.negativeAcknowledge(m.getMessageId());
			}
			return MessageResult.acknowledge(m.getMessageId());
		}));
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setSubscriptionType(SubscriptionType.Shared);
		DefaultReactivePulsarMessageListenerContainer<String> container = new DefaultReactivePulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder().maxRedeliverCount(1)
				.deadLetterTopic(deadLetterTopic).build();
		container.setConsumerCustomizer(b -> b.deadLetterPolicy(deadLetterPolicy));
		container.start();
		MutableReactiveMessageSenderSpec prodConfig = new MutableReactiveMessageSenderSpec();
		prodConfig.setTopicName(topic);
		DefaultReactivePulsarSenderFactory<String> pulsarProducerFactory = new DefaultReactivePulsarSenderFactory<>(
				reactivePulsarClient, prodConfig, null);
		ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(pulsarProducerFactory);
		Flux.range(0, 5).map(i -> "hello john doe" + i).as(pulsarTemplate::send).subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		CountDownLatch dlqLatch = new CountDownLatch(1);
		dlqConsumer.consumeOne(messageMono -> messageMono.map(message -> {
			if (message.getValue().endsWith("4")) {
				dlqLatch.countDown();
			}
			return MessageResult.acknowledge(message.getMessageId());
		})).subscribe();

		assertThat(dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		pulsarClient.close();
	}

}

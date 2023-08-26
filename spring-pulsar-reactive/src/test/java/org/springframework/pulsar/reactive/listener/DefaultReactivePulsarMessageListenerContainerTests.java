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

package org.springframework.pulsar.reactive.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.adapter.DefaultMessageGroupingFunction;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for {@link DefaultReactivePulsarMessageListenerContainer}
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class DefaultReactivePulsarMessageListenerContainerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Test
	void oneByOneMessageHandler() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("1");
			var consumerFactory = createAndPrepareConsumerFactory(topic, reactivePulsarClient);
			var latch = new CountDownLatch(1);
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setMessageHandler(
					(ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();
			createPulsarTemplate(topic, reactivePulsarClient).send("hello john doe").subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void streamingMessageHandler() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("2");
			var consumerFactory = createAndPrepareConsumerFactory(topic, reactivePulsarClient);
			var latch = new CountDownLatch(5);
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setMessageHandler(
					(ReactivePulsarStreamingHandler<String>) (msg) -> msg.doOnNext((m) -> latch.countDown())
						.map(MessageResult::acknowledge));
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();
			createPulsarTemplate(topic, reactivePulsarClient)
				.newMessages(Flux.range(0, 5).map(i -> MessageSpec.of("hello john doe" + i)))
				.send()
				.subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void containerPropertiesAreRespected() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("3");
			var consumerFactory = createAndPrepareConsumerFactory(topic, reactivePulsarClient);
			var latch = new CountDownLatch(1);
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setMessageHandler(
					(ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
			containerProperties.setConcurrency(5);
			containerProperties.setUseKeyOrderedProcessing(true);
			containerProperties.setHandlingTimeout(Duration.ofMillis(7));
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();
			createPulsarTemplate(topic, reactivePulsarClient).send("hello john doe").subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(container).extracting("pipeline", InstanceOfAssertFactories.type(ReactiveMessagePipeline.class))
				.hasFieldOrPropertyWithValue("concurrency", 5)
				.hasFieldOrPropertyWithValue("handlingTimeout", Duration.ofMillis(7))
				.extracting("groupingFunction")
				.isInstanceOf(DefaultMessageGroupingFunction.class);
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void createConsumerWithSharedSubTypeOnFactoryWithExclusiveSubType() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("4");
			ReactiveMessageConsumerBuilderCustomizer<String> defaultConfig = (builder) -> {
				builder.topic(topic);
				builder.subscriptionName(topic + "-sub");
			};
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient,
					List.of(defaultConfig));
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setMessageHandler((ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.empty());
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();

			Thread.sleep(2_000);
			StepVerifier
				.create(consumerFactory.createConsumer(Schema.STRING,
						List.of(builder -> builder.subscriptionType(SubscriptionType.Shared)))
					.consumeNothing())
				.expectErrorMatches((ex) -> {
					return true;
				})
				.verify(Duration.ofSeconds(10));
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void createConsumerWithSharedSubTypeOnFactoryWithSharedSubType() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("5");
			ReactiveMessageConsumerBuilderCustomizer<String> defaultConfig = (builder) -> {
				builder.topic(topic);
				builder.subscriptionName(topic + "-sub");
			};
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient,
					List.of(defaultConfig));
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setSubscriptionType(SubscriptionType.Shared);
			containerProperties.setMessageHandler((ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.empty());
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();

			Thread.sleep(2_000);
			StepVerifier
				.create(consumerFactory.createConsumer(Schema.STRING,
						List.of(builder -> builder.subscriptionType(SubscriptionType.Shared)))
					.consumeNothing())
				.expectComplete()
				.verify(Duration.ofSeconds(10));
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void containerPropertiesTopicsPattern() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = "drpmlct-6-foo";
			var subscription = topic + "-sub";
			ReactiveMessageConsumerBuilderCustomizer<String> customizer = (builder) -> {
				builder.topic(topic);
				builder.subscriptionName(topic + "-sub");
			};
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<String>(reactivePulsarClient, null);
			// Ensure subscription is created
			consumerFactory.createConsumer(Schema.STRING, List.of(customizer))
				.consumeNothing()
				.block(Duration.ofSeconds(5));
			var latch = new CountDownLatch(1);
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setTopicsPattern("persistent://public/default/drpmlct-6-.*");
			containerProperties.setSubscriptionName(subscription);
			containerProperties.setMessageHandler(
					(ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();

			createPulsarTemplate(topic, reactivePulsarClient).send("hello john doe").subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	@Test
	void deadLetterTopicCustomizer() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<String> container = null;
		try {
			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = "drpmlct-7";
			var deadLetterTopic = topic + "-dlt";
			ReactiveMessageConsumerBuilderCustomizer<String> defaultConfig = (builder) -> {
				builder.topic(topic);
				builder.subscriptionName(topic + "-sub");
				builder.negativeAckRedeliveryDelay(Duration.ZERO);
			};
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient,
					List.of(defaultConfig));
			var dlqConsumer = consumerFactory.createConsumer(Schema.STRING,
					List.of((builder) -> builder.topics(List.of(deadLetterTopic))));
			// Ensure subscriptions are created
			consumerFactory.createConsumer(Schema.STRING).consumeNothing().block(Duration.ofSeconds(5));
			dlqConsumer.consumeNothing().block(Duration.ofSeconds(5));
			var latch = new CountDownLatch(6);
			var containerProperties = new ReactivePulsarContainerProperties<String>();
			containerProperties.setSchema(Schema.STRING);
			containerProperties.setMessageHandler(
					(ReactivePulsarStreamingHandler<String>) (msg) -> msg.doOnNext((m) -> latch.countDown())
						.map((m) -> m.getValue().endsWith("4") ? MessageResult.negativeAcknowledge(m)
								: MessageResult.acknowledge(m)));
			containerProperties.setSubscriptionType(SubscriptionType.Shared);

			var deadLetterPolicy = DeadLetterPolicy.builder()
				.maxRedeliverCount(1)
				.deadLetterTopic(deadLetterTopic)
				.build();
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.setConsumerCustomizer(b -> b.deadLetterPolicy(deadLetterPolicy));
			container.start();

			var producerFactory = DefaultReactivePulsarSenderFactory.<String>builderFor(reactivePulsarClient)
				.withDefaultTopic(topic)
				.withDefaultConfigCustomizer((builder) -> builder.batchingEnabled(false))
				.build();
			var pulsarTemplate = new ReactivePulsarTemplate<>(producerFactory);
			Flux.range(0, 5).map(i -> MessageSpec.of("hello john doe" + i)).as(pulsarTemplate::send).subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

			var dlqLatch = new CountDownLatch(1);
			dlqConsumer.consumeOne(message -> {
				if (message.getValue().endsWith("4")) {
					dlqLatch.countDown();
				}
				return Mono.just(MessageResult.acknowledge(message));
			}).block();
			assertThat(dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
			pulsarClient.close();
		}
	}

	private String topicNameForTest(String suffix) {
		return "drpmlct-" + suffix;
	}

	private DefaultReactivePulsarConsumerFactory<String> createAndPrepareConsumerFactory(String topic,
			ReactivePulsarClient reactivePulsarClient) {
		ReactiveMessageConsumerBuilderCustomizer<String> defaultConfig = (builder) -> {
			builder.topic(topic);
			builder.subscriptionName(topic + "-sub");
		};
		var consumerFactory = new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient, List.of(defaultConfig));
		// Ensure subscription is created
		consumerFactory.createConsumer(Schema.STRING).consumeNothing().block(Duration.ofSeconds(5));
		return consumerFactory;
	}

	private ReactivePulsarTemplate<String> createPulsarTemplate(String topic,
			ReactivePulsarClient reactivePulsarClient) {
		var producerFactory = DefaultReactivePulsarSenderFactory.<String>builderFor(reactivePulsarClient)
			.withDefaultTopic(topic)
			.build();
		return new ReactivePulsarTemplate<>(producerFactory);
	}

	private void safeStopContainer(ReactivePulsarMessageListenerContainer<?> container) {
		try {
			if (container != null) {
				container.stop();
			}
		}
		catch (Exception ex) {
			logger.warn(ex, "Failed to stop container %s: %s".formatted(container, ex.getMessage()));
		}
	}

}

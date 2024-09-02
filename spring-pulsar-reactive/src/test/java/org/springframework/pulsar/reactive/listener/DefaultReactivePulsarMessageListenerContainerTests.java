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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.JSONSchemaUtil;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordObjectMapper;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

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

	@Test
	void oneByOneMessageHandlerWithCustomObjectMapper() throws Exception {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		ReactivePulsarMessageListenerContainer<UserRecord> container = null;
		try {
			// Prepare the schema with custom object mapper
			var objectMapper = UserRecordObjectMapper.withDeser();
			var schema = JSONSchemaUtil.schemaForTypeWithObjectMapper(UserRecord.class, objectMapper);

			var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
			var topic = topicNameForTest("com-topic");
			var consumerFactory = createAndPrepareConsumerFactory(topic, schema, reactivePulsarClient);
			var containerProperties = new ReactivePulsarContainerProperties<UserRecord>();
			containerProperties.setSchema(schema);
			var latch = new CountDownLatch(1);
			AtomicReference<UserRecord> consumedRecordRef = new AtomicReference<>();
			containerProperties.setMessageHandler((ReactivePulsarOneByOneMessageHandler<UserRecord>) (msg) -> {
				consumedRecordRef.set(msg.getValue());
				return Mono.fromRunnable(latch::countDown);
			});
			container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProperties);
			container.start();

			var sentUserRecord = new UserRecord("person", 51);
			// deser adds '-deser' to name and 5 to age
			var expectedConsumedUser = new UserRecord("person-deser", 56);
			createPulsarTemplate(topic, reactivePulsarClient).send(sentUserRecord).subscribe();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(consumedRecordRef).hasValue(expectedConsumedUser);
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
		return this.createAndPrepareConsumerFactory(topic, Schema.STRING, reactivePulsarClient);
	}

	private <T> DefaultReactivePulsarConsumerFactory<T> createAndPrepareConsumerFactory(String topic, Schema<T> schema,
			ReactivePulsarClient reactivePulsarClient) {
		ReactiveMessageConsumerBuilderCustomizer<T> defaultConfig = (builder) -> {
			builder.topic(topic);
			builder.subscriptionName(topic + "-sub");
		};
		var consumerFactory = new DefaultReactivePulsarConsumerFactory<T>(reactivePulsarClient, List.of(defaultConfig));
		// Ensure subscription is created
		consumerFactory.createConsumer(schema).consumeNothing().block(Duration.ofSeconds(5));
		return consumerFactory;
	}

	private <T> ReactivePulsarTemplate<T> createPulsarTemplate(String topic,
			ReactivePulsarClient reactivePulsarClient) {
		var producerFactory = DefaultReactivePulsarSenderFactory.<T>builderFor(reactivePulsarClient)
			.withDefaultTopic(topic)
			.build();
		return new ReactivePulsarTemplate<T>(producerFactory);
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

	@SuppressWarnings("unchecked")
	@Nested
	class WithStartupFailures {

		// @Test
		void whenPolicyIsStopThenExceptionIsThrown() throws Exception {
			DefaultReactivePulsarConsumerFactory<String> consumerFactory = mock(
					DefaultReactivePulsarConsumerFactory.class);
			var containerProps = new ReactivePulsarContainerProperties<String>();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.STOP);
			containerProps.setSchema(Schema.STRING);
			containerProps
				.setMessageHandler((ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(() -> {
				}));
			var container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProps);
			// setup factory to throw ex when create consumer
			var failCause = new PulsarException("please-stop");
			when(consumerFactory.createConsumer(any(), any())).thenThrow(failCause);
			// start container and expect ex thrown
			assertThatIllegalStateException().isThrownBy(() -> container.start())
				.withMessageStartingWith("Error starting Reactive pipeline")
				.withCause(failCause);
			assertThat(container.isRunning()).isFalse();
		}

		// @Test
		void whenPolicyIsContinueThenExceptionIsNotThrown() throws Exception {
			DefaultReactivePulsarConsumerFactory<String> consumerFactory = mock(
					DefaultReactivePulsarConsumerFactory.class);
			var containerProps = new ReactivePulsarContainerProperties<String>();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.CONTINUE);
			containerProps.setSchema(Schema.STRING);
			containerProps
				.setMessageHandler((ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(() -> {
				}));
			var container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProps);
			// setup factory to throw ex when create consumer
			var failCause = new PulsarException("please-continue");
			when(consumerFactory.createConsumer(any(), any())).thenThrow(failCause);
			// start container and expect ex thrown
			container.start();
			assertThat(container.isRunning()).isFalse();
		}

		// @Test
		void whenPolicyIsRetryAndRetriesAreExhaustedThenContainerDoesNotStart() throws Exception {
			DefaultReactivePulsarConsumerFactory<String> consumerFactory = mock(
					DefaultReactivePulsarConsumerFactory.class);
			var retryCount = new AtomicInteger(0);
			var thrown = new ArrayList<Throwable>();
			var retryListener = new RetryListener() {
				@Override
				public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					retryCount.set(context.getRetryCount());
				}

				@Override
				public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					thrown.add(throwable);
				}
			};
			var retryTemplate = RetryTemplate.builder()
				.maxAttempts(2)
				.fixedBackoff(Duration.ofSeconds(1))
				.withListener(retryListener)
				.build();
			var containerProps = new ReactivePulsarContainerProperties<String>();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			containerProps.setSchema(Schema.STRING);
			containerProps
				.setMessageHandler((ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(() -> {
				}));
			var container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProps);
			// setup factory to throw ex when create consumer
			var failCause = new PulsarException("please-retry-exhausted");
			doThrow(failCause).doThrow(failCause).doThrow(failCause).when(consumerFactory).createConsumer(any(), any());
			// start container and expect ex not thrown and 2 retries
			container.start();
			await().atMost(Duration.ofSeconds(15)).until(() -> retryCount.get() == 2);
			assertThat(thrown).containsExactly(failCause, failCause);
			assertThat(container.isRunning()).isFalse();
			// factory called 3x (initial + 2 retries)
			verify(consumerFactory, times(3)).createConsumer(any(), any());
		}

		// @Test
		void whenPolicyIsRetryAndRetryIsSuccessfulThenContainerStarts() throws Exception {
			var pulsarClient = PulsarClient.builder()
				.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build();
			ReactivePulsarMessageListenerContainer<String> container = null;
			try {
				var reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);
				var topic = topicNameForTest("wsf-retry");
				var subscription = topic + "-sub";
				var consumerFactory = spy(
						new DefaultReactivePulsarConsumerFactory<String>(reactivePulsarClient, List.of((cb) -> {
							cb.topic(topic);
							cb.subscriptionName(subscription);
						})));
				var retryCount = new AtomicInteger(0);
				var thrown = new ArrayList<Throwable>();
				var retryListener = new RetryListener() {
					@Override
					public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
							Throwable throwable) {
						retryCount.set(context.getRetryCount());
					}

					@Override
					public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
							Throwable throwable) {
						thrown.add(throwable);
					}
				};
				var retryTemplate = RetryTemplate.builder()
					.maxAttempts(3)
					.fixedBackoff(Duration.ofSeconds(1))
					.withListener(retryListener)
					.build();
				var latch = new CountDownLatch(1);
				var containerProps = new ReactivePulsarContainerProperties<String>();
				containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
				containerProps.setStartupFailureRetryTemplate(retryTemplate);
				containerProps.setMessageHandler(
						(ReactivePulsarOneByOneMessageHandler<String>) (msg) -> Mono.fromRunnable(latch::countDown));
				containerProps.setSchema(Schema.STRING);
				container = new DefaultReactivePulsarMessageListenerContainer<>(consumerFactory, containerProps);

				// setup factory to throw ex on initial call and 1st retry - then succeed
				// on
				// 2nd retry
				var failCause = new PulsarException("please-retry");
				doThrow(failCause).doThrow(failCause)
					.doCallRealMethod()
					.when(consumerFactory)
					.createConsumer(any(), any());
				// start container and expect started after retries
				container.start();
				await().atMost(Duration.ofSeconds(240)).until(container::isRunning);

				// factory called 3x (initial call + 2 retries)
				verify(consumerFactory, times(3)).createConsumer(any(), any());
				// only had to retry once (2nd call in retry template succeeded)
				assertThat(retryCount).hasValue(1);
				assertThat(thrown).containsExactly(failCause);
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

	}

}

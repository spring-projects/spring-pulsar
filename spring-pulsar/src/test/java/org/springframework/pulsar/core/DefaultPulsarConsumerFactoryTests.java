/*
 * Copyright 2023-present the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Tests for {@link DefaultPulsarConsumerFactory}.
 *
 * @author Chris Bono
 */
class DefaultPulsarConsumerFactoryTests implements PulsarTestContainerSupport {

	private static final Schema<String> SCHEMA = Schema.STRING;

	@Nullable protected PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (pulsarClient != null && !pulsarClient.isClosed()) {
			pulsarClient.close();
		}
	}

	@Nested
	class CreateConsumerUsingFactoryWithoutDefaultConfig {

		private DefaultPulsarConsumerFactory<String> consumerFactory;

		@BeforeEach
		void createConsumerFactory() {
			consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, null);
		}

		@Test
		void withoutSchema() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(null, Collections.singletonList("topic0"), null, null, null))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("Schema must be specified");
		}

		@SuppressWarnings("resource")
		@Test
		void withSchemaOnly() {
			assertThatThrownBy(() -> consumerFactory.createConsumer(SCHEMA, null, null, null, null))
				.isInstanceOf(PulsarException.class)
				.hasCauseInstanceOf(InvalidConfigurationException.class)
				.hasMessageContaining("Topic name must be set on the consumer builder");
		}

		@SuppressWarnings("resource")
		@Test
		void withSchemaAndTopics() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"), null, null, null))
				.isInstanceOf(PulsarException.class)
				.hasCauseInstanceOf(InvalidConfigurationException.class)
				.hasMessageContaining("Subscription name must be set on the consumer builder");
		}

		@Test
		void withSchemaAndTopicsAndSubscription() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"),
					"topic0-sub", null, null)) {
				assertThat(consumer.getTopic()).isEqualTo("topic0");
				assertThat(consumer.getSubscription()).isEqualTo("topic0-sub");
			}
		}

		@Test
		void withSchemaAndTopicsAndCustomizerWithSubscription() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"), null, null,
					List.of((cb) -> cb.subscriptionName("topic0-sub")))) {
				assertThat(consumer.getTopic()).isEqualTo("topic0");
				assertThat(consumer.getSubscription()).isEqualTo("topic0-sub");
			}
		}

		@Test
		void withMetadataProperties() throws PulsarClientException {
			var metadataProperties = Collections.singletonMap("foo", "bar");
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"),
					"topic0-sub", metadataProperties, List.of((cb) -> cb.subscriptionName("topic0-sub")))) {
				assertThat(consumer).hasFieldOrPropertyWithValue("metadata", metadataProperties);
			}
		}

		@Test
		void withSingleCustomizerApi() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"),
					"topic0-sub", (cb) -> cb.consumerName("foo-consumer"))) {
				assertThat(consumer.getConsumerName()).isEqualTo("foo-consumer");
			}
		}

		@Test
		void customizesAreAppliedLast() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"),
					"topic0-sub", null, List.of((cb) -> cb.subscriptionName("topic0-sub2")))) {
				assertThat(consumer.getSubscription()).isEqualTo("topic0-sub2");
			}
		}

		@SuppressWarnings("unchecked")
		@Test
		void customizedAreAppliedInOrder() throws PulsarClientException {
			ConsumerBuilderCustomizer<String> customizer1 = mock(ConsumerBuilderCustomizer.class);
			ConsumerBuilderCustomizer<String> customizer2 = mock(ConsumerBuilderCustomizer.class);
			try (var ignored = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"), "topic0-sub",
					null, List.of(customizer1, customizer2))) {
				InOrder inOrder = inOrder(customizer1, customizer2);
				inOrder.verify(customizer1).customize(any(ConsumerBuilder.class));
				inOrder.verify(customizer2).customize(any(ConsumerBuilder.class));
			}
		}

	}

	@Nested
	class CreateConsumerUsingFactoryWithDefaultConfig {

		private final String defaultTopic = "dft-topic";

		private final String defaultSubscription = "dft-topic-sub";

		private final Map<String, String> defaultMetadataProperties = Collections.singletonMap("foo", "bar");

		private DefaultPulsarConsumerFactory<String> consumerFactory;

		@BeforeEach
		void createConsumerFactory() {
			consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of((consumerBuilder) -> {
				consumerBuilder.topic(defaultTopic);
				consumerBuilder.subscriptionName(defaultSubscription);
				consumerBuilder.properties(defaultMetadataProperties);
			}));
		}

		@Test
		void withoutSchema() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(null, Collections.singletonList("topic0"), null, null, null))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("Schema must be specified");
		}

		@Test
		void withSchemaOnly() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, null, null, null, null)) {
				assertThat(consumer.getTopic()).isEqualTo(defaultTopic);
				assertThat(consumer.getSubscription()).isEqualTo(defaultSubscription);
				assertThat(consumer).hasFieldOrPropertyWithValue("metadata", defaultMetadataProperties);
			}
		}

		@Test
		void withSchemaAndTopics() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"), null, null,
					null)) {
				assertThat(consumer.getTopic()).isEqualTo("topic0");
				assertThat(consumer.getSubscription()).isEqualTo(defaultSubscription);
			}
		}

		@Test
		void withSchemaAndTopicsAndSubscription() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"),
					"topic0-sub", null, null)) {
				assertThat(consumer.getTopic()).isEqualTo("topic0");
				assertThat(consumer.getSubscription()).isEqualTo("topic0-sub");
			}
		}

		@Test
		void withMetadataProperties() throws PulsarClientException {
			var metadataProperties = Collections.singletonMap("hello", "world");
			try (var consumer = consumerFactory.createConsumer(SCHEMA, null, null, metadataProperties, null)) {
				assertThat(consumer).hasFieldOrPropertyWithValue("metadata", metadataProperties);
			}
		}

		@Test
		void customizesAreAppliedLast() throws PulsarClientException {
			try (var consumer = consumerFactory.createConsumer(SCHEMA, null, null, null,
					List.of((cb) -> cb.subscriptionName("topic0-sub2")))) {
				assertThat(consumer.getSubscription()).isEqualTo("topic0-sub2");
			}
		}

		@Nested
		@SuppressWarnings("unchecked")
		class DefaultConfigCustomizerApi {

			private ConsumerBuilderCustomizer<String> configCustomizer1 = mock(ConsumerBuilderCustomizer.class);

			private ConsumerBuilderCustomizer<String> configCustomizer2 = mock(ConsumerBuilderCustomizer.class);

			private ConsumerBuilderCustomizer<String> createConsumerCustomizer = mock(ConsumerBuilderCustomizer.class);

			@Test
			void singleConfigCustomizer() throws PulsarClientException {
				try (var ignored = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of(configCustomizer1))
					.createConsumer(SCHEMA, List.of("topic0"), "dft-sub", createConsumerCustomizer)) {
					InOrder inOrder = inOrder(configCustomizer1, createConsumerCustomizer);
					inOrder.verify(configCustomizer1).customize(any(ConsumerBuilder.class));
					inOrder.verify(createConsumerCustomizer).customize(any(ConsumerBuilder.class));
				}
			}

			@Test
			void multipleConfigCustomizers() throws PulsarClientException {
				try (var ignored = new DefaultPulsarConsumerFactory<>(pulsarClient,
						List.of(configCustomizer2, configCustomizer1))
					.createConsumer(SCHEMA, List.of("topic0"), "dft-sub", createConsumerCustomizer)) {
					InOrder inOrder = inOrder(configCustomizer1, configCustomizer2, createConsumerCustomizer);
					inOrder.verify(configCustomizer2).customize(any(ConsumerBuilder.class));
					inOrder.verify(configCustomizer1).customize(any(ConsumerBuilder.class));
					inOrder.verify(createConsumerCustomizer).customize(any(ConsumerBuilder.class));
				}
			}

		}

	}

	@Nested
	class CreateConsumerWithTopicBuilder {

		@Test
		void ensureTopicNamesFullyQualified() throws PulsarClientException {
			var pulsarTopicBuilder = spy(new PulsarTopicBuilder());
			DefaultPulsarConsumerFactory<String> consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
					null);
			consumerFactory.setTopicBuilder(pulsarTopicBuilder);
			try (Consumer<String> consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic1"),
					"with-pulsar-topic-builder-ensure-topic-names-fully-qualified-sub", null, null)) {
				assertThat(consumer.getTopic()).isEqualTo("persistent://public/default/topic1");
				verify(pulsarTopicBuilder).getFullyQualifiedNameForTopic("topic1");
			}
		}

		@Test
		void ensureTopicsPatternFullyQualified() throws PulsarClientException {
			var pulsarTopicBuilder = spy(new PulsarTopicBuilder());
			ConsumerBuilderCustomizer<String> customizer = (builder) -> builder.topicsPattern("topic-.*");
			DefaultPulsarConsumerFactory<String> consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
					List.of(customizer));
			consumerFactory.setTopicBuilder(pulsarTopicBuilder);

			try (var consumer = consumerFactory.createConsumer(SCHEMA, null,
					"with-pulsar-topic-builder-ensure-topics-pattern-fully-qualified-sub", null, null)) {
				assertThat(consumer).isInstanceOf(PatternMultiTopicsConsumerImpl.class);
				var patternMultiTopicsConsumer = (PatternMultiTopicsConsumerImpl<String>) consumer;
				var topicsPattern = patternMultiTopicsConsumer.getPattern();
				assertThat(topicsPattern.inputPattern()).isEqualTo("persistent://public/default/topic-.*");
				verify(pulsarTopicBuilder).getFullyQualifiedNameForTopic("topic-.*");
				temporarilyDealWithPulsar24698(patternMultiTopicsConsumer);
			}
		}

		@Test
		void ensureDeadLetterPolicyTopicsFullyQualified() throws PulsarClientException {
			var pulsarTopicBuilder = spy(new PulsarTopicBuilder());
			var deadLetterTopic = "with-pulsar-topic-builder-ensure-dlp-dlt-fq";
			var retryLetterTopic = "%s-retry".formatted(deadLetterTopic);
			var deadLetterPolicy = DeadLetterPolicy.builder()
				.maxRedeliverCount(2)
				.deadLetterTopic(deadLetterTopic)
				.retryLetterTopic(retryLetterTopic)
				.build();
			ConsumerBuilderCustomizer<String> customizer = (builder) -> builder.deadLetterPolicy(deadLetterPolicy);
			DefaultPulsarConsumerFactory<String> consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
					List.of(customizer));
			consumerFactory.setTopicBuilder(pulsarTopicBuilder);

			try (Consumer<String> consumer = consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic1"),
					"%s-sub".formatted(deadLetterTopic), null, null)) {
				assertThat(consumer).extracting("deadLetterPolicy.deadLetterTopic")
					.isEqualTo("persistent://public/default/%s".formatted(deadLetterTopic));
				assertThat(consumer).extracting("deadLetterPolicy.retryLetterTopic")
					.isEqualTo("persistent://public/default/%s".formatted(retryLetterTopic));
				verify(pulsarTopicBuilder).getFullyQualifiedNameForTopic(deadLetterTopic);
				verify(pulsarTopicBuilder).getFullyQualifiedNameForTopic(retryLetterTopic);
			}
		}

		// TODO remove when Pulsar client updates to 4.2.0
		private void temporarilyDealWithPulsar24698(PatternMultiTopicsConsumerImpl<String> consumer) {
			// See https://github.com/apache/pulsar/pull/24698
			// If this is not here there will be numerous exceptions when
			// PulsarClient.close
			CompletableFuture<?> watcherFuture = assertThat(consumer)
				.extracting("watcherFuture", InstanceOfAssertFactories.type(CompletableFuture.class))
				.actual();
			watcherFuture.join();

		}

	}

}

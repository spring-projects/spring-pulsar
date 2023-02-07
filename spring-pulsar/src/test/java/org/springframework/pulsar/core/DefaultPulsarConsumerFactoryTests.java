/*
 * Copyright 2023-2023 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Tests for {@link DefaultPulsarConsumerFactory}.
 *
 * @author Chris Bono
 */
class DefaultPulsarConsumerFactoryTests implements PulsarTestContainerSupport {

	private static final Schema<String> SCHEMA = Schema.STRING;

	@Nullable
	protected PulsarClient pulsarClient;

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
			consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, Collections.emptyMap());
		}

		@Test
		void withoutSchema() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(null, Collections.singletonList("topic0"), null, null, null))
							.isInstanceOf(NullPointerException.class).hasMessageContaining("Schema must be specified");
		}

		@SuppressWarnings("resource")
		@Test
		void withSchemaOnly() {
			assertThatThrownBy(() -> consumerFactory.createConsumer(SCHEMA, null, null, null, null))
					.isInstanceOf(InvalidConfigurationException.class)
					.hasMessageContaining("Topic name must be set on the consumer builder");
		}

		@SuppressWarnings("resource")
		@Test
		void withSchemaAndTopics() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(SCHEMA, Collections.singletonList("topic0"), null, null, null))
							.isInstanceOf(InvalidConfigurationException.class)
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
			Map<String, Object> defaultConfig = new HashMap<>();
			defaultConfig.put("topicNames", Collections.singleton(defaultTopic));
			defaultConfig.put("properties", defaultMetadataProperties);
			defaultConfig.put("subscriptionName", defaultSubscription);
			consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, defaultConfig);
		}

		@Test
		void withoutSchema() {
			assertThatThrownBy(
					() -> consumerFactory.createConsumer(null, Collections.singletonList("topic0"), null, null, null))
							.isInstanceOf(NullPointerException.class).hasMessageContaining("Schema must be specified");
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

	}

}

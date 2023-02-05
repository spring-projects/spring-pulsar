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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Common tests for {@link DefaultPulsarProducerFactory} and
 * {@link CachingPulsarProducerFactory}.
 *
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 */
abstract class PulsarProducerFactoryTests implements PulsarTestContainerSupport {

	protected final Schema<String> schema = Schema.STRING;

	protected PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@SuppressWarnings("ConstantConditions")
	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (pulsarClient != null && !pulsarClient.isClosed()) {
			pulsarClient.close();
		}
	}

	private void assertThatProducerHasSchemaAndTopic(Producer<String> producer, Schema<String> expectedSchema,
			String expectedTopic) {
		producer = actualProducer(producer);
		assertThat(producer).hasFieldOrPropertyWithValue("schema", expectedSchema);
		assertThat(producer.getTopic()).isEqualTo(expectedTopic);
	}

	private void assertThatProducerHasTopic(Producer<String> producer, String expectedTopic) {
		producer = actualProducer(producer);
		assertThat(producer.getTopic()).isEqualTo(expectedTopic);
	}

	protected void assertThatProducerHasEncryptionKeys(Producer<String> producer, Set<String> encryptionKeys) {
		producer = actualProducer(producer);
		assertThat(producer).extracting("conf")
				.asInstanceOf(InstanceOfAssertFactories.type(ProducerConfigurationData.class))
				.extracting(ProducerConfigurationData::getEncryptionKeys).isEqualTo(encryptionKeys);
	}

	protected PulsarProducerFactory<String> newProducerFactory() {
		return producerFactory(pulsarClient, Collections.emptyMap());
	}

	protected PulsarProducerFactory<String> newProducerFactoryWithDefaultTopic(String defaultTopic) {
		return producerFactory(pulsarClient, Collections.singletonMap("topicName", defaultTopic));
	}

	private PulsarProducerFactory<String> newProducerFactoryWithDefaultKeys(Set<String> defaultKeys) {
		return producerFactory(pulsarClient, Collections.singletonMap("encryptionKeys", defaultKeys));
	}

	/**
	 * By default, echoes the specified producer back to the caller, but subclasses can
	 * override to provide the actual producer if the specified producer is wrapped.
	 * @return the actual producer if the specified producer is wrapped
	 */
	protected Producer<String> actualProducer(Producer<String> producer) {
		return producer;
	}

	/**
	 * Subclasses override to provide concrete {@link PulsarProducerFactory} instance.
	 * @param pulsarClient the Pulsar client
	 * @param producerConfig the Pulsar producers config
	 * @return a Pulsar producer factory instance to use for the tests
	 */
	protected abstract PulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient,
			Map<String, Object> producerConfig);

	@Test
	@SuppressWarnings("unchecked")
	void createProducerWithAllOptions() throws PulsarClientException {
		var keys = Set.of("key");
		ProducerBuilderCustomizer<String> customizer1 = mock(ProducerBuilderCustomizer.class);
		var producerFactory = newProducerFactory();
		try (var producer = producerFactory.createProducer(schema, "topic0", keys,
				Collections.singletonList(customizer1))) {
			assertThatProducerHasSchemaAndTopic(producer, schema, "topic0");
			assertThatProducerHasEncryptionKeys(producer, keys);
			verify(customizer1).customize(any(ProducerBuilder.class));
		}
	}

	@Nested
	class CreateProducerSchemaAndTopicApi {

		@Test
		void withoutSchema() {
			assertThatNullPointerException().isThrownBy(() -> newProducerFactory().createProducer(null, "topic0"))
					.withMessageContaining("Schema must be specified");
		}

		@Test
		void topicSpecifiedWithoutDefaultTopic() throws PulsarClientException {
			try (var producer = newProducerFactory().createProducer(schema, "topic1")) {
				assertThatProducerHasTopic(producer, "topic1");
			}
		}

		@Test
		void topicSpecifiedWithDefaultTopic() throws PulsarClientException {
			var producerFactory = newProducerFactoryWithDefaultTopic("topic0");
			try (var producer = producerFactory.createProducer(schema, "topic1")) {
				assertThatProducerHasSchemaAndTopic(producer, schema, "topic1");
			}
		}

		@Test
		void noTopicSpecifiedWithoutDefaultTopic() {
			assertThatIllegalArgumentException().isThrownBy(() -> newProducerFactory().createProducer(schema, null))
					.withMessageContaining("Topic must be specified when no default topic is configured");
		}

		@Test
		void noTopicSpecifiedWithDefaultTopic() throws PulsarClientException {
			var producerFactory = newProducerFactoryWithDefaultTopic("topic0");
			try (var producer = producerFactory.createProducer(schema, null)) {
				assertThatProducerHasTopic(producer, "topic0");
			}
		}

	}

	@Nested
	@SuppressWarnings("unchecked")
	class CreateProducerCustomizerApi {

		private ProducerBuilderCustomizer<String> customizer1 = mock(ProducerBuilderCustomizer.class);

		private ProducerBuilderCustomizer<String> customizer2 = mock(ProducerBuilderCustomizer.class);

		@Test
		void singleCustomizer() throws PulsarClientException {
			try (var producer = newProducerFactory().createProducer(schema, "topic0", customizer1)) {
				assertThatProducerHasSchemaAndTopic(producer, schema, "topic0");
				verify(customizer1).customize(any(ProducerBuilder.class));
			}
		}

		@Test
		void singleCustomizerViaListApi() throws PulsarClientException {
			try (var producer = newProducerFactory().createProducer(schema, "topic0", null,
					Collections.singletonList(customizer1))) {
				assertThatProducerHasSchemaAndTopic(producer, schema, "topic0");
				verify(customizer1).customize(any(ProducerBuilder.class));
			}
		}

		@Test
		void multipleCustomizers() throws PulsarClientException {
			try (var ignored = newProducerFactory().createProducer(schema, "topic0", null,
					Arrays.asList(customizer1, customizer2))) {
				InOrder inOrder = inOrder(customizer1, customizer2);
				inOrder.verify(customizer1).customize(any(ProducerBuilder.class));
				inOrder.verify(customizer2).customize(any(ProducerBuilder.class));
			}
		}

		@Test
		void customizerThatSetsTopicHasNoEffect() throws PulsarClientException {
			try (var producer = newProducerFactory().createProducer(schema, "topic1", (b) -> b.topic("topic-5150"))) {
				assertThatProducerHasTopic(producer, "topic1");
			}
		}

	}

	@Nested
	class CreateProducerEncryptionKeysApi {

		@Test
		void withDefaultEncryptionKeys() throws PulsarClientException {
			var keys = Set.of("key");
			var producerFactory = newProducerFactoryWithDefaultKeys(keys);
			try (var producer = producerFactory.createProducer(schema, "topic0")) {
				assertThatProducerHasEncryptionKeys(producer, keys);
			}
		}

		@Test
		void specificEncryptionKeys() throws PulsarClientException {
			var keys = Set.of("key");
			var producerFactory = newProducerFactory();
			try (var producer = producerFactory.createProducer(schema, "topic0", keys, null)) {
				assertThatProducerHasEncryptionKeys(producer, keys);
			}
		}

	}

}

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
import org.junit.jupiter.api.Test;

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

	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (pulsarClient != null && !pulsarClient.isClosed()) {
			pulsarClient.close();
		}
	}

	@Test
	void createProducerWithSpecificTopic() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		try (Producer<String> producer = producerFactory.createProducer(schema, "topic1")) {
			assertProducerHasTopicSchemaAndEncryptionKeys(producer, "topic1", schema, Collections.emptySet());
		}
	}

	@Test
	void createProducerWithDefaultTopic() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient,
				Collections.singletonMap("topicName", "topic0"));
		try (Producer<String> producer = producerFactory.createProducer(schema, null)) {
			assertProducerHasTopicSchemaAndEncryptionKeys(producer, "topic0", schema, Collections.emptySet());
		}
	}

	@Test
	void createProducerWithDefaultEncryptionKeys() throws PulsarClientException {
		Set<String> encryptionKeys = Set.of("key");
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient,
				Collections.singletonMap("encryptionKeys", encryptionKeys));
		try (Producer<String> producer = producerFactory.createProducer(schema, "topic0")) {
			assertProducerHasTopicSchemaAndEncryptionKeys(producer, "topic0", schema, encryptionKeys);
		}
	}

	@Test
	void createProducerWithSpecificEncryptionKeys() throws PulsarClientException {
		Set<String> encryptionKeys = Set.of("key");
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		try (Producer<String> producer = producerFactory.createProducer(schema, "topic0", encryptionKeys, null)) {
			assertProducerHasTopicSchemaAndEncryptionKeys(producer, "topic0", schema, encryptionKeys);
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void createProducerWithSingleProducerCustomizer() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		ProducerBuilderCustomizer<String> producerCustomizer = mock(ProducerBuilderCustomizer.class);
		try (Producer<String> producer = producerFactory.createProducer(schema, "topic0", null,
				Collections.singletonList(producerCustomizer))) {
			verify(producerCustomizer).customize(any(ProducerBuilder.class));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void createProducerWithMultipleProducerCustomizers() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		ProducerBuilderCustomizer<String> producerCustomizer1 = mock(ProducerBuilderCustomizer.class);
		ProducerBuilderCustomizer<String> producerCustomizer2 = mock(ProducerBuilderCustomizer.class);
		try (Producer<String> producer = producerFactory.createProducer(schema, "topic0", null,
				Arrays.asList(producerCustomizer1, producerCustomizer2))) {
			verify(producerCustomizer1).customize(any(ProducerBuilder.class));
			verify(producerCustomizer2).customize(any(ProducerBuilder.class));
		}
	}

	@Test
	void createProducerWithNoTopic() {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		assertThatThrownBy(() -> producerFactory.createProducer(schema, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Topic must be specified when no default topic is configured");
	}

	protected void assertProducerHasTopicSchemaAndEncryptionKeys(Producer<String> producer, String topic,
			Schema<String> schema, Set<String> encryptionKeys) {
		assertThat(producer.getTopic()).isEqualTo(topic);
		assertThat(producer).hasFieldOrPropertyWithValue("schema", schema);
		assertThat(producer).extracting("conf")
				.asInstanceOf(InstanceOfAssertFactories.type(ProducerConfigurationData.class))
				.extracting(ProducerConfigurationData::getEncryptionKeys).isEqualTo(encryptionKeys);
	}

	/**
	 * Subclasses override to provide concrete {@link PulsarProducerFactory} instance.
	 * @param pulsarClient the Pulsar client
	 * @param producerConfig the Pulsar producers config
	 * @return a Pulsar producer factory instance to use for the tests
	 */
	protected abstract PulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient,
			Map<String, Object> producerConfig);

}

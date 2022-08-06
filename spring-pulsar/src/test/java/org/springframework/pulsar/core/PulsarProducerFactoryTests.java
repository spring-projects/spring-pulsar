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
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
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
 */
abstract class PulsarProducerFactoryTests extends AbstractContainerBaseTests {

	protected final Schema<String> schema = Schema.STRING;

	protected PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		pulsarClient = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build();
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
		try (Producer<String> producer = producerFactory.createProducer("topic1", schema)) {
			assertProducerHasTopicSchemaAndRouter(producer, "topic1", schema, null);
		}
	}

	@Test
	void createProducerWithSpecificTopicAndMessageRouter() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		MessageRouter router = mock(MessageRouter.class);
		try (Producer<String> producer = producerFactory.createProducer("topic1", schema, router)) {
			assertProducerHasTopicSchemaAndRouter(producer, "topic1", schema, router);
		}
	}

	@Test
	void createProducerWithDefaultTopic() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient,
				Collections.singletonMap("topicName", "topic0"));
		try (Producer<String> producer = producerFactory.createProducer(null, schema)) {
			assertProducerHasTopicSchemaAndRouter(producer, "topic0", schema, null);
		}
	}

	@Test
	void createProducerWithDefaultTopicAndMessageRouter() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient,
				Collections.singletonMap("topicName", "topic0"));
		MessageRouter router = mock(MessageRouter.class);
		try (Producer<String> producer = producerFactory.createProducer(null, schema, router)) {
			assertProducerHasTopicSchemaAndRouter(producer, "topic0", schema, router);
		}
	}

	@Test
	void createProducerWithNoTopic() {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		assertThatThrownBy(() -> producerFactory.createProducer(null, schema))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Topic must be specified when no default topic is configured");
	}

	protected void assertProducerHasTopicSchemaAndRouter(Producer<String> producer, String topic, Schema<String> schema,
			MessageRouter router) {
		assertThat(producer.getTopic()).isEqualTo(topic);
		assertThat(producer).hasFieldOrPropertyWithValue("schema", schema);
		assertThat(producer).extracting("conf")
				.asInstanceOf(InstanceOfAssertFactories.type(ProducerConfigurationData.class))
				.extracting(ProducerConfigurationData::getCustomMessageRouter).isSameAs(router);
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

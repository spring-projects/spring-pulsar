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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;

import org.springframework.lang.Nullable;

/**
 * Tests for {@link DefaultPulsarProducerFactory}.
 *
 * @author Chris Bono
 */
class DefaultPulsarProducerFactoryTests extends PulsarProducerFactoryTests {

	@Test
	void createProducerMultipleTimeDoesNotCacheProducer() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = newProducerFactory();
		try (Producer<String> producer1 = producerFactory.createProducer(schema, "topic1")) {
			try (Producer<String> producer2 = producerFactory.createProducer(schema, "topic1")) {
				try (Producer<String> producer3 = producerFactory.createProducer(schema, "topic1")) {
					assertThat(producer1).isNotSameAs(producer2).isNotSameAs(producer3);
				}
			}
		}
	}

	@Override
	protected PulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient, @Nullable String defaultTopic,
			@Nullable ProducerBuilderCustomizer<String> defaultConfigCustomizer) {
		return new DefaultPulsarProducerFactory<>(pulsarClient, defaultTopic, defaultConfigCustomizer);
	}

}

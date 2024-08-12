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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

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
			@Nullable List<ProducerBuilderCustomizer<String>> defaultConfigCustomizers,
			@Nullable PulsarTopicBuilder topicBuilder) {
		var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, defaultTopic, defaultConfigCustomizers,
				new DefaultTopicResolver());
		producerFactory.setTopicBuilder(topicBuilder);
		return producerFactory;
	}

	@Nested
	@SuppressWarnings("unchecked")
	class DefaultConfigCustomizerApi {

		private ProducerBuilderCustomizer<String> configCustomizer1 = mock(ProducerBuilderCustomizer.class);

		private ProducerBuilderCustomizer<String> configCustomizer2 = mock(ProducerBuilderCustomizer.class);

		private ProducerBuilderCustomizer<String> createProducerCustomizer = mock(ProducerBuilderCustomizer.class);

		@Test
		void singleConfigCustomizer() throws PulsarClientException {
			try (var ignored = newProducerFactoryWithDefaultConfigCustomizers(List.of(configCustomizer1))
				.createProducer(schema, "topic0", createProducerCustomizer)) {
				InOrder inOrder = inOrder(configCustomizer1, createProducerCustomizer);
				inOrder.verify(configCustomizer1).customize(any(ProducerBuilder.class));
				inOrder.verify(createProducerCustomizer).customize(any(ProducerBuilder.class));
			}
		}

		@Test
		void multipleConfigCustomizers() throws PulsarClientException {
			try (var ignored = newProducerFactoryWithDefaultConfigCustomizers(
					List.of(configCustomizer2, configCustomizer1))
				.createProducer(schema, "topic0", createProducerCustomizer)) {
				InOrder inOrder = inOrder(configCustomizer1, configCustomizer2, createProducerCustomizer);
				inOrder.verify(configCustomizer2).customize(any(ProducerBuilder.class));
				inOrder.verify(configCustomizer1).customize(any(ProducerBuilder.class));
				inOrder.verify(createProducerCustomizer).customize(any(ProducerBuilder.class));
			}
		}

	}

}

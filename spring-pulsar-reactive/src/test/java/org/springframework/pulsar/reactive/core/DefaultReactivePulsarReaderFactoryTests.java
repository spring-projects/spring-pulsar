/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.reactive.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.PulsarTopicBuilder;

/**
 * Tests for {@link DefaultReactivePulsarReaderFactory}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class DefaultReactivePulsarReaderFactoryTests {

	private static final Schema<String> schema = Schema.STRING;

	@Test
	void createReader() {
		DefaultReactivePulsarReaderFactory<String> readerFactory = new DefaultReactivePulsarReaderFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null),
				List.of((builder) -> builder.readerName("test-reader")));

		ReactiveMessageReader<String> reader = readerFactory.createReader(schema);

		assertThat(reader).extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
			.extracting(ReactiveMessageReaderSpec::getReaderName)
			.isEqualTo("test-reader");
	}

	@Test
	void createReaderWithCustomizer() {
		DefaultReactivePulsarReaderFactory<String> readerFactory = new DefaultReactivePulsarReaderFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null),
				List.of((builder) -> builder.readerName("test-reader")));

		ReactiveMessageReader<String> reader = readerFactory.createReader(schema,
				Collections.singletonList(builder -> builder.readerName("new-test-reader")));

		assertThat(reader).extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
			.extracting(ReactiveMessageReaderSpec::getReaderName)
			.isEqualTo("new-test-reader");
	}

	@Test
	void createReaderUsingTopicBuilder() {
		var inputTopic = "my-topic";
		var fullyQualifiedTopic = "persistent://public/default/my-topic";
		var topicBuilder = spy(new PulsarTopicBuilder());
		var readerFactory = new DefaultReactivePulsarReaderFactory<String>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), null);
		readerFactory.setTopicBuilder(topicBuilder);
		var reader = readerFactory.createReader(schema,
				Collections.singletonList(builder -> builder.topic(inputTopic)));
		assertThat(reader).extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
			.extracting(ReactiveMessageReaderSpec::getTopicNames)
			.isEqualTo(List.of(fullyQualifiedTopic));
		verify(topicBuilder).getFullyQualifiedNameForTopic(inputTopic);
	}

}

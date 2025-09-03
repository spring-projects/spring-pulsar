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
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.PulsarTopicBuilder;

/**
 * Tests for {@link DefaultReactivePulsarConsumerFactory}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class DefaultReactivePulsarConsumerFactoryTests {

	private static final Schema<String> SCHEMA = Schema.STRING;

	@Nested
	class FactoryCreatedWithoutSpec {

		private DefaultReactivePulsarConsumerFactory<String> consumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), null);

		@Test
		void createConsumer() {
			ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA);

			assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.isNotNull();
		}

		@Test
		void createConsumerWithCustomizer() {
			ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA,
					Collections.singletonList(builder -> builder.consumerName("new-test-consumer")));

			assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.extracting(ReactiveMessageConsumerSpec::getConsumerName)
				.isEqualTo("new-test-consumer");
		}

	}

	@Nested
	class FactoryCreatedWithSpec {

		private org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory<String> consumerFactory;

		@BeforeEach
		void createConsumerFactory() {
			consumerFactory = new DefaultReactivePulsarConsumerFactory<>(
					AdaptedReactivePulsarClientFactory.create((PulsarClient) null),
					List.of((builder) -> builder.consumerName("test-consumer")));
		}

		@Test
		void createConsumer() {
			ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA);

			assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.extracting(ReactiveMessageConsumerSpec::getConsumerName)
				.isEqualTo("test-consumer");
		}

		@Test
		void createConsumerWithCustomizer() {
			ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA,
					Collections.singletonList(builder -> builder.consumerName("new-test-consumer")));

			assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.extracting(ReactiveMessageConsumerSpec::getConsumerName)
				.isEqualTo("new-test-consumer");
		}

	}

	@Nested
	class FactoryCreatedWithTopicBuilder {

		@Test
		void createConsumerEnsureTopicNamesFullyQualified() {
			var topicBuilder = spy(new PulsarTopicBuilder());
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<String>(
					AdaptedReactivePulsarClientFactory.create((PulsarClient) null), null);
			consumerFactory.setTopicBuilder(topicBuilder);
			var inputTopic = "my-topic";
			var fullyQualifiedTopic = "persistent://public/default/my-topic";
			var consumer = consumerFactory.createConsumer(SCHEMA,
					Collections.singletonList(builder -> builder.topic(inputTopic)));
			assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.hasFieldOrPropertyWithValue("topicNames", List.of(fullyQualifiedTopic));
			verify(topicBuilder).getFullyQualifiedNameForTopic(inputTopic);
		}

		@Test
		void createConsumerEnsureTopicsPatternFullyQualified() {
			var topicBuilder = spy(new PulsarTopicBuilder());
			var consumerFactory = new DefaultReactivePulsarConsumerFactory<String>(
					AdaptedReactivePulsarClientFactory.create((PulsarClient) null), null);
			consumerFactory.setTopicBuilder(topicBuilder);
			var inputTopicsPattern = "my-topic-.*";
			var fullyQualifiedTopicsPattern = "persistent://public/default/my-topic-.*";
			var consumer = consumerFactory.createConsumer(SCHEMA,
					Collections.singletonList(builder -> builder.topicsPattern(Pattern.compile(inputTopicsPattern))));
			ReactiveMessageConsumerSpec reactiveMessageConsumerSpec = assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.actual();
			assertThat(reactiveMessageConsumerSpec.getTopicsPattern().pattern()).isEqualTo(fullyQualifiedTopicsPattern);
			verify(topicBuilder).getFullyQualifiedNameForTopic(inputTopicsPattern);
		}

	}

}

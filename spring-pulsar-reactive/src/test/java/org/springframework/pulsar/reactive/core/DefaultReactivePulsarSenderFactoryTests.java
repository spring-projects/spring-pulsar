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

package org.springframework.pulsar.reactive.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.core.TopicResolver;

/**
 * Unit tests for {@link DefaultReactivePulsarSenderFactory}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class DefaultReactivePulsarSenderFactoryTests {

	protected final Schema<String> schema = Schema.STRING;

	@Test
	void createSenderWithCache() {
		ReactiveMessageSenderCache cache = AdaptedReactivePulsarClientFactory.createCache();
		var sender = newSenderFactoryWithCache(cache).createSender(schema, "topic1");
		assertThat(sender).extracting("producerCache").isSameAs(cache);
	}

	@Test
	void createSenderWithTopicResolver() {
		var customTopicResolver = mock(TopicResolver.class);
		var senderFactory = DefaultReactivePulsarSenderFactory.<String>builderFor(mock(ReactivePulsarClient.class))
			.withTopicResolver(customTopicResolver)
			.build();
		assertThat(senderFactory).hasFieldOrPropertyWithValue("topicResolver", customTopicResolver);
	}

	private void assertThatSenderHasTopic(ReactiveMessageSender<String> sender, String expectedTopic) {
		assertThatSenderSpecSatisfies(sender,
				(senderSpec) -> assertThat(senderSpec).extracting(ReactiveMessageSenderSpec::getTopicName)
					.isEqualTo(expectedTopic));
	}

	private void assertThatSenderSpecSatisfies(ReactiveMessageSender<String> sender,
			ThrowingConsumer<ReactiveMessageSenderSpec> specConsumer) {
		assertThat(sender).extracting("senderSpec", InstanceOfAssertFactories.type(ReactiveMessageSenderSpec.class))
			.satisfies(specConsumer);
	}

	private ReactivePulsarSenderFactory<String> newSenderFactory() {
		return DefaultReactivePulsarSenderFactory.<String>builderFor(mock(PulsarClient.class)).build();
	}

	private ReactivePulsarSenderFactory<String> newSenderFactoryWithDefaultTopic(String defaultTopic) {
		return DefaultReactivePulsarSenderFactory.<String>builderFor(mock(PulsarClient.class))
			.withDefaultTopic(defaultTopic)
			.build();
	}

	private ReactivePulsarSenderFactory<String> newSenderFactoryWithCache(ReactiveMessageSenderCache cache) {
		return DefaultReactivePulsarSenderFactory.<String>builderFor(mock(PulsarClient.class))
			.withMessageSenderCache(cache)
			.build();
	}

	@Nested
	class CreateSenderSchemaAndTopicApi {

		@Test
		void withoutSchema() {
			assertThatNullPointerException().isThrownBy(() -> newSenderFactory().createSender(null, "topic0"))
				.withMessageContaining("Schema must be specified");
		}

		@Test
		void topicSpecifiedWithDefaultTopic() {
			var sender = newSenderFactoryWithDefaultTopic("topic0").createSender(schema, "topic1");
			assertThatSenderHasTopic(sender, "topic1");
		}

		@Test
		void topicSpecifiedWithoutDefaultTopic() {
			var sender = newSenderFactory().createSender(schema, "topic1");
			assertThatSenderHasTopic(sender, "topic1");
		}

		@Test
		void noTopicSpecifiedWithDefaultTopic() {
			var sender = newSenderFactoryWithDefaultTopic("topic0").createSender(schema, null);
			assertThatSenderHasTopic(sender, "topic0");
		}

		@Test
		void noTopicSpecifiedWithoutDefaultTopic() {
			assertThatIllegalArgumentException().isThrownBy(() -> newSenderFactory().createSender(schema, null))
				.withMessageContaining("Topic must be specified when no default topic is configured");
		}

	}

	@Nested
	class CreateSenderCustomizersApi {

		@Test
		void singleCustomizer() {
			var sender = newSenderFactory().createSender(schema, "topic1", (b) -> b.producerName("fooProducer"));
			assertThatSenderSpecSatisfies(sender,
					(senderSpec) -> assertThat(senderSpec.getProducerName()).isEqualTo("fooProducer"));
		}

		@Test
		void singleCustomizerViaListApi() {
			var sender = newSenderFactory().createSender(schema, "topic1",
					Collections.singletonList((b) -> b.producerName("fooProducer")));
			assertThatSenderSpecSatisfies(sender,
					(senderSpec) -> assertThat(senderSpec.getProducerName()).isEqualTo("fooProducer"));
		}

		@Test
		void multipleCustomizers() {
			var sender = newSenderFactory().createSender(schema, "topic1",
					Arrays.asList((b) -> b.producerName("fooProducer"), (b) -> b.compressionType(CompressionType.LZ4)));
			assertThatSenderSpecSatisfies(sender, (senderSpec) -> {
				assertThat(senderSpec.getProducerName()).isEqualTo("fooProducer");
				assertThat(senderSpec.getCompressionType()).isEqualTo(CompressionType.LZ4);
			});
		}

		@Test
		void customizerThatSetsTopicHasNoEffect() {
			var sender = newSenderFactory().createSender(schema, "topic1", (b) -> b.topic("topic-5150"));
			assertThatSenderHasTopic(sender, "topic1");
		}

	}

	@Nested
	@SuppressWarnings("unchecked")
	class DefaultConfigCustomizerApi {

		private ReactiveMessageSenderBuilderCustomizer<String> configCustomizer1 = mock(
				ReactiveMessageSenderBuilderCustomizer.class);

		private ReactiveMessageSenderBuilderCustomizer<String> configCustomizer2 = mock(
				ReactiveMessageSenderBuilderCustomizer.class);

		private ReactiveMessageSenderBuilderCustomizer<String> createSenderCustomizer = mock(
				ReactiveMessageSenderBuilderCustomizer.class);

		@Test
		void singleConfigCustomizer() {
			newSenderFactoryWithCustomizers(List.of(configCustomizer1)).createSender(schema, "topic1",
					List.of(createSenderCustomizer));
			InOrder inOrder = inOrder(configCustomizer1, createSenderCustomizer);
			inOrder.verify(configCustomizer1).customize(any(ReactiveMessageSenderBuilder.class));
			inOrder.verify(createSenderCustomizer).customize(any(ReactiveMessageSenderBuilder.class));
		}

		@Test
		void multipleConfigCustomizers() {
			newSenderFactoryWithCustomizers(List.of(configCustomizer2, configCustomizer1)).createSender(schema,
					"topic1", List.of(createSenderCustomizer));
			InOrder inOrder = inOrder(configCustomizer1, configCustomizer2, createSenderCustomizer);
			inOrder.verify(configCustomizer2).customize(any(ReactiveMessageSenderBuilder.class));
			inOrder.verify(configCustomizer1).customize(any(ReactiveMessageSenderBuilder.class));
			inOrder.verify(createSenderCustomizer).customize(any(ReactiveMessageSenderBuilder.class));
		}

		private ReactivePulsarSenderFactory<String> newSenderFactoryWithCustomizers(
				List<ReactiveMessageSenderBuilderCustomizer<String>> customizers) {
			return DefaultReactivePulsarSenderFactory.<String>builderFor(mock(PulsarClient.class))
				.withDefaultConfigCustomizers(customizers)
				.build();
		}

	}

	@Nested
	class RestartFactoryTests {

		@Test
		void restartLifecycle() throws Exception {
			var cache = spy(AdaptedReactivePulsarClientFactory.createCache());
			var senderFactory = (DefaultReactivePulsarSenderFactory<String>) newSenderFactoryWithCache(cache);
			senderFactory.start();
			senderFactory.createSender(schema, "topic1");
			senderFactory.stop();
			senderFactory.stop();
			verify(cache, times(1)).close();
			clearInvocations(cache);
			senderFactory.start();
			senderFactory.createSender(schema, "topic2");
			senderFactory.stop();
			verify(cache, times(1)).close();
		}

	}

}

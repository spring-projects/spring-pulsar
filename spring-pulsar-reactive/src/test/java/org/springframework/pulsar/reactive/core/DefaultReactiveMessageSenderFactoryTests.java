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

import java.util.Arrays;
import java.util.Collections;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultReactivePulsarSenderFactory}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class DefaultReactiveMessageSenderFactoryTests {

	protected final Schema<String> schema = Schema.STRING;

	@Test
	void createSenderWithCache() {
		ReactiveMessageSenderCache cache = AdaptedReactivePulsarClientFactory.createCache();
		var sender = newSenderFactoryWithCache(cache).createSender(schema, "topic1");
		assertThat(sender).extracting("producerCache").isSameAs(cache);
	}

	private void assertThatSenderHasTopic(ReactiveMessageSender<String> sender, String expectedTopic) {
		assertThatSenderSpecSatisfies(sender, (senderSpec) -> assertThat(senderSpec)
				.extracting(ReactiveMessageSenderSpec::getTopicName).isEqualTo(expectedTopic));
	}

	private void assertThatSenderSpecSatisfies(ReactiveMessageSender<String> sender,
			ThrowingConsumer<ReactiveMessageSenderSpec> specConsumer) {
		assertThat(sender).extracting("senderSpec", InstanceOfAssertFactories.type(ReactiveMessageSenderSpec.class))
				.satisfies(specConsumer);
	}

	private ReactivePulsarSenderFactory<String> newSenderFactory() {
		return new DefaultReactivePulsarSenderFactory<>((PulsarClient) null, null, null);
	}

	private ReactivePulsarSenderFactory<String> newSenderFactoryWithDefaultTopic(String defaultTopic) {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName(defaultTopic);
		return new DefaultReactivePulsarSenderFactory<>((PulsarClient) null, senderSpec, null);
	}

	private ReactivePulsarSenderFactory<String> newSenderFactoryWithCache(ReactiveMessageSenderCache cache) {
		return new DefaultReactivePulsarSenderFactory<>((PulsarClient) null, null, cache);
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

}

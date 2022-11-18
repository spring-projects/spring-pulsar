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

package org.springframework.pulsar.reactive.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/**
 * Tests for
 * {@link org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory}
 *
 * @author Christophe Bornet
 */
class DefaultReactiveMessageSenderFactoryTests {

	protected final Schema<String> schema = Schema.STRING;

	@Test
	void createSenderWithSpecificTopic() {
		testCreateSender(null, null, "topic1", null, "topic1");
	}

	@Test
	void createSenderWithDefaultTopic() {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName("topic0");

		testCreateSender(senderSpec, null, null, null, "topic0");
	}

	@Test
	void createSenderWithSingleSenderCustomizer() {
		testCreateSender(null, null, "topic1", Collections.singletonList(builder -> builder.topic("topic1")), "topic1");
	}

	@Test
	void createSenderWithMultipleSenderCustomizer() {
		org.springframework.pulsar.reactive.core.ReactiveMessageSenderBuilderCustomizer<String> customizer1 = builder -> builder
				.topic("topic1");
		ReactiveMessageSenderCache cache = AdaptedReactivePulsarClientFactory.createCache();
		org.springframework.pulsar.reactive.core.ReactiveMessageSenderBuilderCustomizer<String> customizer2 = builder -> builder
				.cache(cache);

		ReactiveMessageSender<String> sender = testCreateSender(null, null, "topic0",
				Arrays.asList(customizer1, customizer2), "topic1");
		assertThat(sender).extracting("producerCache").isSameAs(cache);
	}

	@Test
	void createSenderWithNoTopic() {
		org.springframework.pulsar.reactive.core.ReactivePulsarSenderFactory<String> senderFactory = new org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory<>(
				(PulsarClient) null, null, null);
		assertThatIllegalArgumentException().isThrownBy(() -> senderFactory.createSender(null, schema))
				.withMessageContaining("Topic must be specified when no default topic is configured");
	}

	@Test
	void createSenderWithCache() {
		ReactiveMessageSenderCache cache = AdaptedReactivePulsarClientFactory.createCache();
		ReactiveMessageSender<String> sender = testCreateSender(null, cache, "topic1", null, "topic1");
		assertThat(sender).extracting("producerCache").isSameAs(cache);
	}

	private ReactiveMessageSender<String> testCreateSender(ReactiveMessageSenderSpec spec,
			ReactiveMessageSenderCache cache, String topic,
			List<ReactiveMessageSenderBuilderCustomizer<String>> customizers, String expectedTopic) {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(
				(PulsarClient) null, spec, cache);
		ReactiveMessageSender<String> sender = senderFactory.createSender(topic, schema, customizers);
		assertThat(sender).extracting("senderSpec", InstanceOfAssertFactories.type(ReactiveMessageSenderSpec.class))
				.extracting(ReactiveMessageSenderSpec::getTopicName).isEqualTo(expectedTopic);
		return sender;
	}

}

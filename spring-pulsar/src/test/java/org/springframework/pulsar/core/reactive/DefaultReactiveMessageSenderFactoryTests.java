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

package org.springframework.pulsar.core.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

/**
 * Common tests for {@link DefaultReactivePulsarSenderFactory}
 *
 * @author Christophe Bornet
 */
class DefaultReactiveMessageSenderFactoryTests {

	protected final Schema<String> schema = Schema.STRING;

	@Test
	void createSenderWithSpecificTopic() {
		testCreateSender(null, null, "topic1", null, null, "topic1", null);
	}

	@Test
	void createSenderWithSpecificTopicAndMessageRouter() {
		MessageRouter router = mock(MessageRouter.class);

		testCreateSender(null, null, "topic1", router, null, "topic1", router);
	}

	@Test
	void createSenderWithDefaultTopic() {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName("topic0");

		testCreateSender(senderSpec, null, null, null, null, "topic0", null);
	}

	@Test
	void createSenderWithDefaultTopicAndMessageRouter() {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName("topic0");
		MessageRouter router = mock(MessageRouter.class);

		testCreateSender(senderSpec, null, null, router, null, "topic0", router);

	}

	@Test
	void createSenderWithSingleSenderCustomizer() {
		testCreateSender(null, null, "topic1", null, Collections.singletonList(builder -> builder.topic("topic1")),
				"topic1", null);
	}

	@Test
	void createSenderWithMultipleSenderCustomizer() {
		ReactiveMessageSenderBuilderCustomizer<String> customizer1 = builder -> builder.topic("topic1");
		MessageRouter router = mock(MessageRouter.class);
		ReactiveMessageSenderBuilderCustomizer<String> customizer2 = builder -> builder.messageRouter(router);

		testCreateSender(null, null, "topic0", null, Arrays.asList(customizer1, customizer2), "topic1", router);
	}

	@Test
	void createSenderWithNoTopic() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(
				(PulsarClient) null, null, null);
		assertThatIllegalArgumentException().isThrownBy(() -> senderFactory.createReactiveMessageSender(null, schema))
				.withMessageContaining("Topic must be specified when no default topic is configured");
	}

	@Test
	void createSenderWithCache() {
		testCreateSender(null, AdaptedReactivePulsarClientFactory.createCache(), "topic1", null, null, "topic1", null);
	}

	private void testCreateSender(ReactiveMessageSenderSpec spec, ReactiveMessageSenderCache cache, String topic,
			MessageRouter router, List<ReactiveMessageSenderBuilderCustomizer<String>> customizers,
			String expectedTopic, MessageRouter expectedRouter) {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(
				(PulsarClient) null, spec, cache);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender(topic, schema, router,
				customizers);
		ObjectAssert<ReactiveMessageSenderSpec> objectAssert = assertThat(sender).extracting("senderSpec")
				.asInstanceOf(InstanceOfAssertFactories.type(ReactiveMessageSenderSpec.class));
		objectAssert.extracting(ReactiveMessageSenderSpec::getTopicName).isEqualTo(expectedTopic);
		objectAssert.extracting(ReactiveMessageSenderSpec::getMessageRouter).isSameAs(expectedRouter);
		assertThat(sender).extracting("producerCache").isSameAs(cache);
	}

}

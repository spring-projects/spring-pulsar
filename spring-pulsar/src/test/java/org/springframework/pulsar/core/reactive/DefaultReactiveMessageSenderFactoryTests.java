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

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;

/**
 * Common tests for {@link DefaultReactivePulsarSenderFactory}
 *
 * @author Christophe Bornet
 */
@SuppressWarnings("unchecked")
class DefaultReactiveMessageSenderFactoryTests {

	protected final Schema<String> schema = Schema.STRING;

	private ProducerBuilder<String> producerBuilder;

	private PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() {
		pulsarClient = mock(PulsarClient.class);
		producerBuilder = mock(ProducerBuilder.class);
		Producer<String> producer = mock(Producer.class);
		TypedMessageBuilder<String> mockMessage = mock(TypedMessageBuilder.class);

		when(mockMessage.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.latest));
		when(producer.newMessage()).thenReturn(mockMessage);
		when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
		when(producerBuilder.createAsync()).thenReturn(CompletableFuture.completedFuture(producer));
		when(pulsarClient.newProducer(schema)).thenReturn(producerBuilder);
	}

	@Test
	void createProducerWithSpecificTopic() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				null);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender("topic1", schema);
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic1", null);
	}

	@Test
	void createProducerWithSpecificTopicAndMessageRouter() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				null);
		MessageRouter router = mock(MessageRouter.class);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender("topic1", schema, router);
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic1", router);
	}

	@Test
	void createProducerWithDefaultTopic() {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName("topic0");
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				senderSpec);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender(null, schema);
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic0", null);
	}

	@Test
	void createProducerWithDefaultTopicAndMessageRouter() {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		senderSpec.setTopicName("topic0");
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				senderSpec);
		MessageRouter router = mock(MessageRouter.class);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender(null, schema, router);
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic0", router);
	}

	@Test
	void createProducerWithSingleProducerCustomizer() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				null);
		ReactiveMessageSenderBuilderCustomizer<String> customizer = builder -> builder.topic("topic1");
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender("topic0", schema, null,
				Collections.singletonList(customizer));
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic1", null);
	}

	@Test
	void createProducerWithMultipleProducerCustomizer() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				null);
		ReactiveMessageSenderBuilderCustomizer<String> customizer1 = builder -> builder.topic("topic1");
		MessageRouter router = mock(MessageRouter.class);
		ReactiveMessageSenderBuilderCustomizer<String> customizer2 = builder -> builder.messageRouter(router);
		ReactiveMessageSender<String> sender = senderFactory.createReactiveMessageSender("topic0", schema, null,
				Arrays.asList(customizer1, customizer2));
		sender.sendMessage(Mono.just(MessageSpec.of("test"))).block(Duration.ofSeconds(5));
		assertSenderHasTopicAndRouter("topic1", router);
	}

	@Test
	void createProducerWithNoTopic() {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(pulsarClient,
				null);
		assertThatIllegalArgumentException().isThrownBy(() -> senderFactory.createReactiveMessageSender(null, schema))
				.withMessageContaining("Topic must be specified when no default topic is configured");
	}

	protected void assertSenderHasTopicAndRouter(String topic, MessageRouter router) {
		verify(producerBuilder).topic(topic);
		if (router != null) {
			verify(producerBuilder).messageRouter(router);
		}
		else {
			verify(producerBuilder, never()).messageRouter(any());
		}
	}

}

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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code PulsarTemplate}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
class PulsarTemplateTests extends AbstractContainerBaseTests {

	private static final String SAMPLE_MESSAGE_KEY = "sample-key";

	private static final TypedMessageBuilderCustomizer<String> sampleMessageKeyCustomizer = messageBuilder -> messageBuilder
			.key(SAMPLE_MESSAGE_KEY);

	@ParameterizedTest(name = "{0}")
	@MethodSource("sendMessageTestProvider")
	void sendMessageTest(String topic, Map<String, Object> producerConfig, SendHandler<Object> handler,
			TypedMessageBuilderCustomizer<String> typedMessageBuilderCustomizer, MessageRouter router)
			throws Exception {
		String subscription = topic + "-sub";
		String msgPayload = topic + "-msg";
		if (router != null) {
			try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
				admin.topics().createPartitionedTopic("persistent://public/default/" + topic, 1);
			}
		}
		try (PulsarClient client = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build()) {
			try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
					.subscriptionName(subscription).subscribe()) {
				PulsarProducerFactory<String> producerFactory = new DefaultPulsarProducerFactory<>(client,
						producerConfig);
				PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory);

				Object sendResponse = handler.doSend(pulsarTemplate, topic, msgPayload, typedMessageBuilderCustomizer,
						router);
				if (sendResponse instanceof CompletableFuture) {
					sendResponse = ((CompletableFuture<?>) sendResponse).get(3, TimeUnit.SECONDS);
				}
				assertThat(sendResponse).isNotNull();

				CompletableFuture<Message<String>> receiveMsgFuture = consumer.receiveAsync();
				Message<String> msg = receiveMsgFuture.get(3, TimeUnit.SECONDS);
				if (typedMessageBuilderCustomizer != null) {
					assertThat(msg.getKey()).isEqualTo(SAMPLE_MESSAGE_KEY);
				}
				assertThat(msg.getData()).asString().isEqualTo(msgPayload);

				// Make sure the producer was closed by the template (albeit indirectly as
				// client removes closed producers)
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(client).extracting("producers")
						.asInstanceOf(InstanceOfAssertFactories.COLLECTION).isEmpty());
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("interceptorInvocationTestProvider")
	void interceptorInvocationTest(String topic, List<ProducerInterceptor> interceptors) throws Exception {
		try (PulsarClient client = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build()) {
			PulsarProducerFactory<String> producerFactory = new DefaultPulsarProducerFactory<>(client,
					Collections.singletonMap("topicName", topic));
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory, interceptors);
			pulsarTemplate.send("test-interceptor");
			for (ProducerInterceptor interceptor : interceptors) {
				verify(interceptor, atLeastOnce()).eligible(any(Message.class));
			}
		}
	}

	@Test
	void sendMessageWithSpecificSchemaTest() throws Exception {
		String topic = "smt-specific-schema-topic";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic)
					.subscriptionName("test-specific-schema-subscription").subscribe()) {
				PulsarProducerFactory<Foo> producerFactory = new DefaultPulsarProducerFactory<>(client,
						Collections.singletonMap("topicName", topic));
				PulsarTemplate<Foo> pulsarTemplate = new PulsarTemplate<>(producerFactory);
				pulsarTemplate.setSchema(Schema.JSON(Foo.class));
				Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
				pulsarTemplate.send(foo);
				assertThat(consumer.receiveAsync()).succeedsWithin(Duration.ofSeconds(3)).extracting(Message::getValue)
						.isEqualTo(foo);
			}
		}
	}

	private static Stream<Arguments> sendMessageTestProvider() {
		return Stream.of(
				arguments("sendMessageToDefaultTopic",
						Collections.singletonMap("topicName", "sendMessageToDefaultTopic"),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.send(),
						null, null),
				arguments("sendMessageToDefaultTopicWithSimpleApi",
						Collections.singletonMap("topicName", "sendMessageToDefaultTopicWithSimpleApi"),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.send(msg), null,
						null),
				arguments("sendMessageToDefaultTopicWithRouter",
						Collections.singletonMap("topicName", "sendMessageToDefaultTopicWithRouter"),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withCustomRouter(router).send(),
						null, mockRouter()),
				arguments("sendMessageToDefaultTopicWithCustomizer",
						Collections.singletonMap("topicName", "sendMessageToDefaultTopicWithCustomizer"),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withMessageCustomizer(customizer).send(),
						sampleMessageKeyCustomizer, null),
				arguments("sendMessageToDefaultTopicWithCustomizerAndRouter",
						Collections.singletonMap("topicName", "sendMessageToDefaultTopicWithCustomizerAndRouter"),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withMessageCustomizer(customizer).withCustomRouter(router).send(),
						sampleMessageKeyCustomizer, mockRouter()),
				arguments("sendMessageToSpecificTopic", Collections.emptyMap(),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withTopic(topic).send(),
						null, null),
				arguments("sendMessageToSpecificTopicWithSimpleApi", Collections.emptyMap(),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.send(topic,
								msg),
						null, null),
				arguments("sendMessageToSpecificTopicWithRouter", Collections.emptyMap(),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withTopic(topic).withCustomRouter(router).send(),
						null, mockRouter()),
				arguments("sendMessageToSpecificTopicWithCustomizer", Collections.emptyMap(),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withMessageCustomizer(customizer).withTopic(topic).send(),
						sampleMessageKeyCustomizer, null),
				arguments("sendMessageToSpecificTopicWithCustomizerAndRouter", Collections.emptyMap(),
						(SendHandler<MessageId>) (template, topic, msg, customizer, router) -> template.newMessage(msg)
								.withMessageCustomizer(customizer).withTopic(topic).withCustomRouter(router).send(),
						sampleMessageKeyCustomizer, mockRouter()),
				arguments("sendAsyncMessageToDefaultTopic",
						Collections.singletonMap("topicName", "sendAsyncMessageToDefaultTopic"),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).sendAsync(),
						null, null),
				arguments("sendAsyncMessageToDefaultTopicWithSimpleApi",
						Collections.singletonMap("topicName", "sendAsyncMessageToDefaultTopicWithSimpleApi"),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.sendAsync(msg),
						null, null),
				arguments("sendAsyncMessageToDefaultTopicWithRouter",
						Collections.singletonMap("topicName", "sendAsyncMessageToDefaultTopicWithRouter"),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withCustomRouter(router).sendAsync(),
						null, mockRouter()),
				arguments("sendAsyncMessageToDefaultTopicWithCustomizer",
						Collections.singletonMap("topicName", "sendAsyncMessageToDefaultTopicWithCustomizer"),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withMessageCustomizer(customizer).sendAsync(),
						sampleMessageKeyCustomizer, null),
				arguments("sendAsyncMessageToDefaultTopicWithCustomizerAndRouter",
						Collections.singletonMap("topicName", "sendAsyncMessageToDefaultTopicWithCustomizerAndRouter"),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withMessageCustomizer(customizer)
										.withCustomRouter(router).sendAsync(),
						sampleMessageKeyCustomizer, mockRouter()),
				arguments("sendAsyncMessageToSpecificTopic", Collections.emptyMap(),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withTopic(topic).sendAsync(),
						null, null),
				arguments("sendAsyncMessageToSpecificTopicWithSimpleApi", Collections.emptyMap(),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.sendAsync(topic, msg),
						null, null),
				arguments("sendAsyncMessageToSpecificTopicWithRouter", Collections.emptyMap(),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withTopic(topic).withCustomRouter(router)
										.sendAsync(),
						null, mockRouter()),
				arguments("sendAsyncMessageToSpecificTopicWithCustomizer", Collections.emptyMap(),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withMessageCustomizer(customizer).withTopic(topic)
										.sendAsync(),
						sampleMessageKeyCustomizer, null),
				arguments("sendAsyncMessageToSpecificTopicWithCustomizerAndRouter", Collections.emptyMap(),
						(SendHandler<CompletableFuture<MessageId>>) (template, topic, msg, customizer,
								router) -> template.newMessage(msg).withMessageCustomizer(customizer).withTopic(topic)
										.withCustomRouter(router).sendAsync(),
						sampleMessageKeyCustomizer, mockRouter()));
	}

	private static Stream<Arguments> interceptorInvocationTestProvider() {
		return Stream.of(
				arguments(Named.of("testSingleInterceptor", "iit-topic-1"),
						Collections.singletonList(mock(ProducerInterceptor.class))),
				arguments(Named.of("testMultipleInterceptors", "iit-topic-2"),
						List.of(mock(ProducerInterceptor.class), mock(ProducerInterceptor.class))));
	}

	private static MessageRouter mockRouter() {
		MessageRouter router = mock(MessageRouter.class);
		when(router.choosePartition(any(Message.class), any(TopicMetadata.class))).thenReturn(0);
		return router;
	}

	@FunctionalInterface
	interface SendHandler<V> {

		V doSend(PulsarTemplate<String> template, String topic, String msg,
				TypedMessageBuilderCustomizer<String> typedMessageBuilderCustomizer, MessageRouter router)
				throws PulsarClientException;

	}

	record Foo(String foo, String bar) {
	}

}

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
import static org.mockito.ArgumentMatchers.argThat;
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
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.pulsar.core.PulsarOperations.SendMessageBuilder;

/**
 * Tests for {@code PulsarTemplate}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
class PulsarTemplateTests extends AbstractContainerBaseTests {

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

	private static Stream<Arguments> interceptorInvocationTestProvider() {
		return Stream.of(
				arguments(Named.of("testSingleInterceptor", "iit-topic-1"),
						Collections.singletonList(mock(ProducerInterceptor.class))),
				arguments(Named.of("testMultipleInterceptors", "iit-topic-2"),
						List.of(mock(ProducerInterceptor.class), mock(ProducerInterceptor.class))));
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

	@ParameterizedTest(name = "{0}")
	@MethodSource("sendMessageTestProvider")
	void sendMessageTest(String testName, SendTestArgs testArgs) throws Exception {
		// Use the test args to construct the params to pass to send handler
		String topic = testName;
		String subscription = topic + "-sub";
		String msgPayload = topic + "-msg";
		MessageRouter router = null;
		if (testArgs.useCustomRouter) {
			router = mock(MessageRouter.class);
			when(router.choosePartition(any(Message.class), any(TopicMetadata.class))).thenReturn(0);
		}
		TypedMessageBuilderCustomizer<String> messageCustomizer = null;
		if (testArgs.useMessageCustomizer) {
			messageCustomizer = (mb) -> mb.key("foo-key");
		}
		ProducerBuilderCustomizer<String> producerCustomizer = null;
		if (testArgs.useProducerCustomizer) {
			producerCustomizer = (pb) -> pb.producerName("foo-producer");
		}

		if (router != null) {
			try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
				admin.topics().createPartitionedTopic("persistent://public/default/" + topic, 1);
			}
		}
		try (PulsarClient client = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl()).build()) {
			try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
					.subscriptionName(subscription).subscribe()) {
				Map<String, Object> producerConfig = testArgs.useSpecificTopic ? Collections.emptyMap()
						: Collections.singletonMap("topicName", topic);
				PulsarProducerFactory<String> producerFactory = new DefaultPulsarProducerFactory<>(client,
						producerConfig);
				PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory);
				Object sendResponse;
				if (testArgs.useSimpleApi) {
					if (testArgs.useAsyncSend) {
						sendResponse = testArgs.useSpecificTopic ? pulsarTemplate.sendAsync(topic, msgPayload)
								: pulsarTemplate.sendAsync(msgPayload);
					}
					else {
						sendResponse = testArgs.useSpecificTopic ? pulsarTemplate.send(topic, msgPayload)
								: pulsarTemplate.send(msgPayload);
					}
				}
				else {
					SendMessageBuilder<String> messageBuilder = pulsarTemplate.newMessage(msgPayload);
					if (testArgs.useSpecificTopic) {
						messageBuilder = messageBuilder.withTopic(topic);
					}
					if (messageCustomizer != null) {
						messageBuilder = messageBuilder.withMessageCustomizer(messageCustomizer);
					}
					if (router != null) {
						messageBuilder = messageBuilder.withCustomRouter(router);
					}
					if (producerCustomizer != null) {
						messageBuilder = messageBuilder.withProducerCustomizer(producerCustomizer);
					}
					sendResponse = testArgs.useAsyncSend ? messageBuilder.sendAsync() : messageBuilder.send();
				}

				if (sendResponse instanceof CompletableFuture) {
					sendResponse = ((CompletableFuture<?>) sendResponse).get(3, TimeUnit.SECONDS);
				}
				assertThat(sendResponse).isNotNull();

				CompletableFuture<Message<String>> receiveMsgFuture = consumer.receiveAsync();
				Message<String> msg = receiveMsgFuture.get(3, TimeUnit.SECONDS);

				assertThat(msg.getData()).asString().isEqualTo(msgPayload);
				if (messageCustomizer != null) {
					assertThat(msg.getKey()).isEqualTo("foo-key");
				}
				if (router != null) {
					verify(router).choosePartition(argThat((Message<String> m) -> m.getTopicName().equals(topic)),
							any(TopicMetadata.class));
				}
				if (producerCustomizer != null) {
					assertThat(msg.getProducerName()).isEqualTo("foo-producer");
				}
				// Make sure the producer was closed by the template (albeit indirectly as
				// client removes closed producers)
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(client).extracting("producers")
						.asInstanceOf(InstanceOfAssertFactories.COLLECTION).isEmpty());
			}
		}
	}

	private static Stream<Arguments> sendMessageTestProvider() {
		return Stream.of(arguments("sendMessageToDefaultTopic", SendTestArgs.useSpecificTopic(false)),
				arguments("sendMessageToDefaultTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(false).useSimpleApi(true)),
				arguments("sendMessageToDefaultTopicWithRouter",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true)),
				arguments("sendMessageToDefaultTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(false).useMessageCustomizer(true)),
				arguments("sendMessageToDefaultTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(false).useProducerCustomizer(true)),
				arguments("sendMessageToDefaultTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true).useMessageCustomizer(true)
								.useProducerCustomizer(true)),
				arguments("sendMessageToSpecificTopic", SendTestArgs.useSpecificTopic(true)),
				arguments("sendMessageToSpecificTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(true).useSimpleApi(true)),
				arguments("sendMessageToSpecificTopicWithRouter",
						SendTestArgs.useSpecificTopic(true).useCustomRouter(true)),
				arguments("sendMessageToSpecificTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(true).useMessageCustomizer(true)),
				arguments("sendMessageToSpecificTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(true).useProducerCustomizer(true)),
				arguments("sendMessageToSpecificTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(true).useCustomRouter(true).useMessageCustomizer(true)
								.useProducerCustomizer(true)),
				arguments("sendAsyncMessageToDefaultTopic", SendTestArgs.useSpecificTopic(false).useAsyncSend(true)),
				arguments("sendAsyncMessageToDefaultTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(false).useAsyncSend(true).useSimpleApi(true)),
				arguments("sendAsyncMessageToDefaultTopicWithRouter",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToDefaultTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(false).useMessageCustomizer(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToDefaultTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(false).useProducerCustomizer(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToDefaultTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true).useMessageCustomizer(true)
								.useProducerCustomizer(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToSpecificTopic", SendTestArgs.useSpecificTopic(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToSpecificTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(true).useAsyncSend(true).useSimpleApi(true)),
				arguments("sendAsyncMessageToSpecificTopicWithRouter",
						SendTestArgs.useSpecificTopic(true).useCustomRouter(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToSpecificTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(true).useMessageCustomizer(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToSpecificTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(true).useProducerCustomizer(true).useAsyncSend(true)),
				arguments("sendAsyncMessageToSpecificTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(true).useCustomRouter(true).useMessageCustomizer(true)
								.useProducerCustomizer(true).useAsyncSend(true)));
	}

	static final class SendTestArgs {

		private boolean useSpecificTopic;

		private boolean useCustomRouter;

		private boolean useMessageCustomizer;

		private boolean useProducerCustomizer;

		private boolean useAsyncSend;

		private boolean useSimpleApi;

		private SendTestArgs(boolean useSpecificTopic) {
			this.useSpecificTopic = useSpecificTopic;
		}

		static SendTestArgs useSpecificTopic(boolean useSpecificTopic) {
			return new SendTestArgs(useSpecificTopic);
		}

		SendTestArgs useCustomRouter(boolean useCustomRouter) {
			this.useCustomRouter = useCustomRouter;
			return this;
		}

		SendTestArgs useMessageCustomizer(boolean useMessageCustomizer) {
			this.useMessageCustomizer = useMessageCustomizer;
			return this;
		}

		SendTestArgs useProducerCustomizer(boolean useProducerCustomizer) {
			this.useProducerCustomizer = useProducerCustomizer;
			return this;
		}

		SendTestArgs useAsyncSend(boolean useAsyncSend) {
			this.useAsyncSend = useAsyncSend;
			return this;
		}

		SendTestArgs useSimpleApi(boolean useSimpleApi) {
			this.useSimpleApi = useSimpleApi;
			return this;
		}

	}

	record Foo(String foo, String bar) {
	}

}

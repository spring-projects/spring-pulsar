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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.pulsar.core.PulsarTestContainerSupport;

import reactor.core.publisher.Mono;

/**
 * Tests for {@link ReactivePulsarSenderTemplate}.
 *
 * @author Christophe Bornet
 */
class ReactivePulsarTemplateTests implements PulsarTestContainerSupport {

	@Test
	void sendMessageWithSpecificSchemaTest() throws Exception {
		String topic = "smt-specific-schema-topic-reactive";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic)
					.subscriptionName("test-specific-schema-subscription").subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				senderSpec.setTopicName(topic);
				ReactivePulsarSenderFactory<Foo> producerFactory = new DefaultReactivePulsarSenderFactory<>(client,
						senderSpec);
				ReactivePulsarSenderTemplate<Foo> pulsarTemplate = new ReactivePulsarSenderTemplate<>(producerFactory);
				pulsarTemplate.setSchema(Schema.JSON(Foo.class));
				Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
				pulsarTemplate.send(foo).subscribe();
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
		MessageSpecBuilderCustomizer<String> messageCustomizer = null;
		if (testArgs.useMessageCustomizer) {
			messageCustomizer = (mb) -> mb.key("foo-key");
		}
		ReactiveMessageSenderBuilderCustomizer<String> senderCustomizer = null;
		if (testArgs.useSenderCustomizer) {
			senderCustomizer = (sb) -> sb.producerName("foo-producer");
		}

		if (router != null) {
			try (PulsarAdmin admin = PulsarAdmin.builder()
					.serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl()).build()) {
				admin.topics().createPartitionedTopic("persistent://public/default/" + topic, 1);
			}
		}
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
					.subscriptionName(subscription).subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				if (!testArgs.useSpecificTopic) {
					senderSpec.setTopicName(topic);
				}
				ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(client,
						senderSpec);
				ReactivePulsarSenderTemplate<String> pulsarTemplate = new ReactivePulsarSenderTemplate<>(senderFactory);
				Mono<MessageId> sendResponse;
				if (testArgs.useSimpleApi) {
					sendResponse = testArgs.useSpecificTopic ? pulsarTemplate.send(topic, msgPayload)
							: pulsarTemplate.send(msgPayload);
				}
				else {
					ReactivePulsarSenderTemplate.ReactiveSendMessageBuilderImpl<String> messageBuilder = pulsarTemplate
							.newMessage(msgPayload);
					if (testArgs.useSpecificTopic) {
						messageBuilder = messageBuilder.withTopic(topic);
					}
					if (messageCustomizer != null) {
						messageBuilder = messageBuilder.withMessageCustomizer(messageCustomizer);
					}
					if (router != null) {
						messageBuilder = messageBuilder.withCustomRouter(router);
					}
					if (senderCustomizer != null) {
						messageBuilder = messageBuilder.withSenderCustomizer(senderCustomizer);
					}
					sendResponse = messageBuilder.send();
				}
				sendResponse.subscribe();

				Message<String> msg = consumer.receive(3, TimeUnit.SECONDS);

				assertThat(msg).isNotNull();
				assertThat(msg.getData()).asString().isEqualTo(msgPayload);
				if (messageCustomizer != null) {
					assertThat(msg.getKey()).isEqualTo("foo-key");
				}
				if (router != null) {
					verify(router).choosePartition(argThat((Message<String> m) -> m.getTopicName().equals(topic)),
							any(TopicMetadata.class));
				}
				if (senderCustomizer != null) {
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
		return Stream.of(arguments("sendReactiveMessageToDefaultTopic", SendTestArgs.useSpecificTopic(false)),
				arguments("sendReactiveMessageToDefaultTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(false).useSimpleApi(true)),
				arguments("sendReactiveMessageToDefaultTopicWithRouter",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true)),
				arguments("sendReactiveMessageToDefaultTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(false).useMessageCustomizer(true)),
				arguments("sendReactiveMessageToDefaultTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(false).useSenderCustomizer(true)),
				arguments("sendReactiveMessageToDefaultTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(false).useCustomRouter(true).useMessageCustomizer(true)
								.useSenderCustomizer(true)),
				arguments("sendReactiveMessageToSpecificTopic", SendTestArgs.useSpecificTopic(true)),
				arguments("sendReactiveMessageToSpecificTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(true).useSimpleApi(true)),
				arguments("sendReactiveMessageToSpecificTopicWithRouter",
						SendTestArgs.useSpecificTopic(true).useCustomRouter(true)),
				arguments("sendReactiveMessageToSpecificTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(true).useMessageCustomizer(true)),
				arguments("sendReactiveMessageToSpecificTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(true).useSenderCustomizer(true)),
				arguments("sendReactiveMessageToSpecificTopicWithAllOptions", SendTestArgs.useSpecificTopic(true)
						.useCustomRouter(true).useMessageCustomizer(true).useSenderCustomizer(true)));
	}

	static final class SendTestArgs {

		private boolean useSpecificTopic;

		private boolean useCustomRouter;

		private boolean useMessageCustomizer;

		private boolean useSenderCustomizer;

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

		SendTestArgs useSenderCustomizer(boolean useSenderCustomizer) {
			this.useSenderCustomizer = useSenderCustomizer;
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

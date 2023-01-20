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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.PulsarTestContainerSupport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link org.springframework.pulsar.reactive.core.ReactivePulsarTemplate}.
 *
 * @author Christophe Bornet
 */
class ReactivePulsarTemplateTests implements PulsarTestContainerSupport {

	@Test
	void sendMessagesWithSpecificSchema() throws Exception {
		String topic = "smt-specific-schema-reactive-topic";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic)
					.subscriptionName("smt-specific-schema-reactive-sub").subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				senderSpec.setTopicName(topic);
				org.springframework.pulsar.reactive.core.ReactivePulsarSenderFactory<Foo> producerFactory = new org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory<>(
						client, senderSpec, null);
				org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<Foo> pulsarTemplate = new org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<>(
						producerFactory);
				pulsarTemplate.setSchema(Schema.JSON(Foo.class));

				List<Foo> foos = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					foos.add(new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID()));
				}
				pulsarTemplate.send(Flux.fromIterable(foos)).subscribe();

				for (int i = 0; i < 10; i++) {
					assertThat(consumer.receiveAsync()).succeedsWithin(Duration.ofSeconds(3))
							.extracting(Message::getValue).isEqualTo(foos.get(i));
				}
			}
		}
	}

	@Test
	void sendMessagesWithSpecificSchemaAndCustomTypeMappings() throws Exception {
		String topic = "smt-specific-schema-custom-reactive-topic";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic)
					.subscriptionName("smt-specific-schema-custom-reactive-sub").subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				senderSpec.setTopicName(topic);
				org.springframework.pulsar.reactive.core.ReactivePulsarSenderFactory<Foo> producerFactory = new org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory<>(
						client, senderSpec, null);
				// Custom schema resolver allows not calling setSchema on template
				DefaultSchemaResolver schemaResolver = new DefaultSchemaResolver();
				schemaResolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
				org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<Foo> pulsarTemplate = new org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<>(
						producerFactory, schemaResolver);

				List<Foo> foos = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					foos.add(new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID()));
				}
				pulsarTemplate.send(Flux.fromIterable(foos)).subscribe();

				// TODO figure out why ordering is not preserved when template does not
				// have schema set
				List<Foo> foos2 = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					CompletableFuture<Message<Foo>> receiveFuture = consumer.receiveAsync();
					assertThat(receiveFuture).succeedsWithin(Duration.ofSeconds(3));
					foos2.add(receiveFuture.get().getValue());
				}
				assertThat(foos).containsExactlyInAnyOrderElementsOf(foos2);
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
		MessageSpecBuilderCustomizer<String> messageCustomizer = null;
		if (testArgs.useMessageCustomizer) {
			messageCustomizer = (mb) -> mb.key("foo-key");
		}
		ReactiveMessageSenderBuilderCustomizer<String> senderCustomizer = null;
		if (testArgs.useSenderCustomizer) {
			senderCustomizer = (sb) -> sb.producerName("foo-producer");
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
						senderSpec, null);
				org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<String> pulsarTemplate = new org.springframework.pulsar.reactive.core.ReactivePulsarTemplate<>(
						senderFactory);
				Mono<MessageId> sendResponse;
				if (testArgs.useTemplateSchema) {
					pulsarTemplate.setSchema(Schema.STRING);
				}
				if (testArgs.useSimpleApi) {
					sendResponse = testArgs.useSpecificTopic ? pulsarTemplate.send(topic, msgPayload)
							: pulsarTemplate.send(msgPayload);
				}
				else {
					ReactivePulsarTemplate.SendMessageBuilderImpl<String> messageBuilder = pulsarTemplate
							.newMessage(msgPayload);
					if (testArgs.useSpecificTopic) {
						messageBuilder = messageBuilder.withTopic(topic);
					}
					if (messageCustomizer != null) {
						messageBuilder = messageBuilder.withMessageCustomizer(messageCustomizer);
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
						SendTestArgs.useSpecificTopic(false).useSimpleApi()),
				arguments("sendReactiveMessageToDefaultTopicWithSimpleApiAndTemplateSchema",
						SendTestArgs.useSpecificTopic(false).useSimpleApi().useTemplateSchema()),
				arguments("sendReactiveMessageToDefaultTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(false).useMessageCustomizer()),
				arguments("sendReactiveMessageToDefaultTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(false).useSenderCustomizer()),
				arguments("sendReactiveMessageToDefaultTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(false).useMessageCustomizer().useSenderCustomizer()),
				arguments("sendReactiveMessageToSpecificTopic", SendTestArgs.useSpecificTopic(true)),
				arguments("sendReactiveMessageToSpecificTopicWithSimpleApi",
						SendTestArgs.useSpecificTopic(true).useSimpleApi()),
				arguments("sendReactiveMessageToSpecificTopicWithSimpleApiAndTemplateSchema",
						SendTestArgs.useSpecificTopic(true).useSimpleApi().useTemplateSchema()),
				arguments("sendReactiveMessageToSpecificTopicWithMessageCustomizer",
						SendTestArgs.useSpecificTopic(true).useMessageCustomizer()),
				arguments("sendReactiveMessageToSpecificTopicWithProducerCustomizer",
						SendTestArgs.useSpecificTopic(true).useSenderCustomizer()),
				arguments("sendReactiveMessageToSpecificTopicWithAllOptions",
						SendTestArgs.useSpecificTopic(true).useMessageCustomizer().useSenderCustomizer()));
	}

	static final class SendTestArgs {

		private final boolean useSpecificTopic;

		private boolean useMessageCustomizer;

		private boolean useSenderCustomizer;

		private boolean useSimpleApi;

		private boolean useTemplateSchema;

		private SendTestArgs(boolean useSpecificTopic) {
			this.useSpecificTopic = useSpecificTopic;
		}

		static SendTestArgs useSpecificTopic(boolean useSpecificTopic) {
			return new SendTestArgs(useSpecificTopic);
		}

		SendTestArgs useMessageCustomizer() {
			this.useMessageCustomizer = true;
			return this;
		}

		SendTestArgs useSenderCustomizer() {
			this.useSenderCustomizer = true;
			return this;
		}

		SendTestArgs useSimpleApi() {
			this.useSimpleApi = true;
			return this;
		}

		SendTestArgs useTemplateSchema() {
			this.useTemplateSchema = true;
			return this;
		}

	}

	record Foo(String foo, String bar) {
	}

}

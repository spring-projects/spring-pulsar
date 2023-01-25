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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.PulsarTestContainerSupport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link org.springframework.pulsar.reactive.core.ReactivePulsarTemplate}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class ReactivePulsarTemplateTests implements PulsarTestContainerSupport {

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void sendMessagesWithSpecificSchema(boolean useSimpleApi) throws Exception {
		String topic = "rptt-sendMessagesWithSpecificSchema-" + useSimpleApi + "-topic";
		String sub = "rptt-sendMessagesWithSpecificSchema-" + useSimpleApi + "-sub";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic).subscriptionName(sub)
					.subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				senderSpec.setTopicName(topic);
				ReactivePulsarSenderFactory<Foo> producerFactory = new DefaultReactivePulsarSenderFactory<>(client,
						senderSpec, null);
				ReactivePulsarTemplate<Foo> pulsarTemplate = new ReactivePulsarTemplate<>(producerFactory);

				List<Foo> foos = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					foos.add(new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID()));
				}

				if (useSimpleApi) {
					pulsarTemplate.send(Flux.fromIterable(foos), Schema.JSON(Foo.class)).subscribe();
				}
				else {
					pulsarTemplate.newMessages(Flux.fromIterable(foos)).withSchema(Schema.JSON(Foo.class)).sendMany()
							.subscribe();
				}

				for (int i = 0; i < 10; i++) {
					assertThat(consumer.receiveAsync()).succeedsWithin(Duration.ofSeconds(3))
							.extracting(Message::getValue).isEqualTo(foos.get(i));
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void sendMessagesWithInferredSchema(boolean useSimpleApi) throws Exception {
		String topic = "rptt-sendMessagesWithInferredSchema-" + useSimpleApi + "-topic";
		String sub = "rptt-sendMessagesWithInferredSchema-" + useSimpleApi + "-sub";
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class)).topic(topic).subscriptionName(sub)
					.subscribe()) {
				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				senderSpec.setTopicName(topic);
				ReactivePulsarSenderFactory<Foo> producerFactory = new DefaultReactivePulsarSenderFactory<>(client,
						senderSpec, null);
				// Custom schema resolver allows not specifying the schema when sending
				DefaultSchemaResolver schemaResolver = new DefaultSchemaResolver();
				schemaResolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
				ReactivePulsarTemplate<Foo> pulsarTemplate = new ReactivePulsarTemplate<>(producerFactory,
						schemaResolver);

				List<Foo> foos = new ArrayList<>();
				for (int i = 0; i < 10; i++) {
					foos.add(new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID()));
				}

				if (useSimpleApi) {
					pulsarTemplate.send(Flux.fromIterable(foos)).subscribe();
				}
				else {
					pulsarTemplate.newMessages(Flux.fromIterable(foos)).sendMany().subscribe();
				}

				// TODO figure out if expected to not be ordered when schema not set on
				// template
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
		if (testArgs.messageCustomizer) {
			messageCustomizer = (mb) -> mb.key("foo-key");
		}
		ReactiveMessageSenderBuilderCustomizer<String> senderCustomizer = null;
		if (testArgs.senderCustomizer) {
			senderCustomizer = (sb) -> sb.producerName("foo-sender");
		}
		try (PulsarClient client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
				.build()) {
			try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
					.subscriptionName(subscription).subscribe()) {

				MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
				if (!testArgs.explicitTopic) {
					senderSpec.setTopicName(topic);
				}
				ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(client,
						senderSpec, null);
				ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
				Mono<MessageId> sendResponse;
				if (testArgs.simpleApi) {
					if (testArgs.explicitSchema && testArgs.explicitTopic) {
						sendResponse = pulsarTemplate.send(topic, msgPayload, Schema.STRING);
					}
					else if (testArgs.explicitSchema) {
						sendResponse = pulsarTemplate.send(msgPayload, Schema.STRING);
					}
					else if (testArgs.explicitTopic) {
						sendResponse = pulsarTemplate.send(topic, msgPayload);
					}
					else {
						sendResponse = pulsarTemplate.send(msgPayload);
					}
				}
				else {
					ReactivePulsarTemplate.SendMessageBuilderImpl<String> messageBuilder = pulsarTemplate
							.newMessage(msgPayload);
					if (testArgs.explicitTopic) {
						messageBuilder = messageBuilder.withTopic(topic);
					}
					if (testArgs.explicitSchema) {
						messageBuilder = messageBuilder.withSchema(Schema.STRING);
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
					assertThat(msg.getProducerName()).isEqualTo("foo-sender");
				}
				// Make sure the producer was closed by the template (albeit indirectly as
				// client removes closed producers)
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(client).extracting("producers")
						.asInstanceOf(InstanceOfAssertFactories.COLLECTION).isEmpty());
			}
		}
	}

	private static Stream<Arguments> sendMessageTestProvider() {
		return Stream.of(arguments("simpleReactiveSend", SendTestArgs.simple()),
				arguments("simpleReactiveSendWithTopic", SendTestArgs.simple().topic()),
				arguments("simpleReactiveSendWithSchema", SendTestArgs.simple().schema()),
				arguments("simpleReactiveSendWithTopicAndSchema", SendTestArgs.simple().topic().schema()),
				arguments("fluentReactiveSend", SendTestArgs.fluent()),
				arguments("fluentReactiveSendWithSchema", SendTestArgs.fluent().schema()),
				arguments("fluentReactiveSendWithTopic", SendTestArgs.fluent().topic()),
				arguments("fluentReactiveSendWithMessageCustomizer", SendTestArgs.fluent().messageCustomizer()),
				arguments("fluentReactiveSendWithSenderCustomizer", SendTestArgs.fluent().senderCustomizer()),
				arguments("fluentReactiveSendWithTopicAndSchema", SendTestArgs.fluent().topic().schema()),
				arguments("fluentReactiveSendWithTopicAndSchemaAndCustomizers",
						SendTestArgs.fluent().topic().schema().messageCustomizer().senderCustomizer()));
	}

	static final class SendTestArgs {

		private boolean simpleApi;

		private boolean explicitTopic;

		private boolean explicitSchema;

		private boolean messageCustomizer;

		private boolean senderCustomizer;

		private SendTestArgs(boolean simpleApi) {
			this.simpleApi = simpleApi;
		}

		static SendTestArgs simple() {
			return new SendTestArgs(true);
		}

		static SendTestArgs fluent() {
			return new SendTestArgs(false);
		}

		SendTestArgs topic() {
			this.explicitTopic = true;
			return this;
		}

		SendTestArgs schema() {
			this.explicitSchema = true;
			return this;
		}

		SendTestArgs messageCustomizer() {
			this.messageCustomizer = true;
			return this;
		}

		SendTestArgs senderCustomizer() {
			this.senderCustomizer = true;
			return this;
		}

	}

	record Foo(String foo, String bar) {
	}

}

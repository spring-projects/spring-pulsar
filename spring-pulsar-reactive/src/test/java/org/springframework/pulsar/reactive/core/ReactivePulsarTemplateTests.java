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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.function.ThrowingConsumer;

import reactor.core.publisher.Flux;

/**
 * Tests for {@link ReactivePulsarTemplate}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class ReactivePulsarTemplateTests implements PulsarTestContainerSupport {

	private PulsarClient client;

	@BeforeEach
	void setup() throws PulsarClientException {
		client = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@AfterEach
	void tearDown() throws PulsarClientException {
		// Make sure the producer was closed by the template (albeit indirectly as
		// client removes closed producers)
		await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(client).extracting("producers")
				.asInstanceOf(InstanceOfAssertFactories.COLLECTION).isEmpty());
		client.close();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("sendMessageTestProvider")
	void sendMessageTest(String testName, Consumer<ReactivePulsarTemplate<String>> sendFunction,
			Boolean withDefaultTopic, String expectedValue) throws Exception {
		sendAndConsume(sendFunction, testName, Schema.STRING, expectedValue, withDefaultTopic);
	}

	static Stream<Arguments> sendMessageTestProvider() {
		String message = "test-message";
		Flux<MessageSpec<String>> messagePublisher = Flux.just(MessageSpec.of(message));
		return Stream.of(
				arguments("simpleSendWithDefaultTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.send(message).subscribe(),
						true, message),
				arguments("simpleSendWithTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("simpleSendWithTopic", message).subscribe(),
						false, message),
				arguments("simpleSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.send(message, Schema.STRING)
								.subscribe(),
						true, message),
				arguments("simpleSendWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("simpleSendWithTopicAndSchema", message, Schema.STRING).subscribe(),
						false, message),
				arguments("simpleSendNullWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("simpleSendNullWithTopicAndSchema", (String) null, Schema.STRING).subscribe(),
						false, null),

				arguments("simplePublisherSendWithDefaultTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.send(messagePublisher)
								.subscribe(),
						true, message),
				arguments("simplePublisherSendWithTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("simplePublisherSendWithTopic", messagePublisher).subscribe(),
						false, message),
				arguments("simplePublisherSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send(messagePublisher, Schema.STRING).subscribe(),
						true, message),
				arguments("simplePublisherSendWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("simplePublisherSendWithTopicAndSchema", messagePublisher, Schema.STRING)
								.subscribe(),
						false, message),

				arguments("fluentSendWithDefaultTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.newMessage(message).send()
								.subscribe(),
						true, message),
				arguments("fluentSendWithTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.newMessage(message)
								.withTopic("fluentSendWithTopic").send().subscribe(),
						false, message),
				arguments("fluentSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.newMessage(message)
								.withSchema(Schema.STRING).send().subscribe(),
						true, message),
				arguments("fluentSendNullWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.newMessage(null)
								.withSchema(Schema.STRING).withTopic("fluentSendNullWithTopicAndSchema").send()
								.subscribe(),
						false, null),
				arguments("fluentPublisherSend", (Consumer<ReactivePulsarTemplate<String>>) (template) -> template
						.newMessages(messagePublisher).send().subscribe(), true, message));
	}

	@Test
	void sendMessageWithMessageCustomizer() throws Exception {
		Consumer<ReactivePulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
				.withMessageCustomizer((mb) -> mb.key("test-key")).send().subscribe();
		Message<String> msg = sendAndConsume(sendFunction, "sendMessageWithMessageCustomizer", Schema.STRING,
				"test-message", true);
		assertThat(msg.getKey()).isEqualTo("test-key");
	}

	@Test
	void sendMessageWithSenderCustomizer() throws Exception {
		Consumer<ReactivePulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
				.withSenderCustomizer((sb) -> sb.producerName("test-producer")).send().subscribe();
		Message<String> msg = sendAndConsume(sendFunction, "sendMessageWithSenderCustomizer", Schema.STRING,
				"test-message", true);
		assertThat(msg.getProducerName()).isEqualTo("test-producer");
	}

	@Test
	void sendMessageWithCustomTopicMapping() throws Exception {
		String topic = "sendMessageWithCustomTopicMapping";

		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(client,
				new MutableReactiveMessageSenderSpec(), null);

		DefaultTopicResolver topicResolver = new DefaultTopicResolver();
		topicResolver.addCustomTopicMapping(String.class, topic);
		ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory,
				new DefaultSchemaResolver(), topicResolver);

		Consumer<ReactivePulsarTemplate<String>> sendFunction = (template) -> template.send("test-message").subscribe();
		sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.STRING, "test-message");
	}

	@Test
	void sendMessageWithCustomSchemaMapping() throws Exception {
		String topic = "sendMessageWithCustomSchemaMapping";

		ReactivePulsarSenderFactory<Foo> senderFactory = new DefaultReactivePulsarSenderFactory<>(client,
				new MutableReactiveMessageSenderSpec(), null);

		DefaultSchemaResolver schemaResolver = new DefaultSchemaResolver();
		schemaResolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
		ReactivePulsarTemplate<Foo> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory, schemaResolver,
				new DefaultTopicResolver());

		Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
		Consumer<ReactivePulsarTemplate<Foo>> sendFunction = (template) -> template.send(topic, foo).subscribe();
		sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.JSON(Foo.class), foo);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("sendMessageFailedTestProvider")
	void sendMessageFailed(String testName, ThrowingConsumer<ReactivePulsarTemplate<String>> sendFunction) {
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(client,
				new MutableReactiveMessageSenderSpec(), null);
		ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
		assertThatIllegalArgumentException().isThrownBy(() -> sendFunction.accept(pulsarTemplate));
	}

	static Stream<Arguments> sendMessageFailedTestProvider() {
		String message = "test-message";
		return Stream.of(
				arguments("sendWithoutTopic",
						(ThrowingConsumer<ReactivePulsarTemplate<String>>) (template) -> template.send(message)),
				arguments("sendNullWithoutSchema",
						(ThrowingConsumer<ReactivePulsarTemplate<String>>) (template) -> template
								.send("sendNullWithoutSchema", (String) null)));
	}

	@Test
	void sendNullWithDefaultTopicFails() {
		MutableReactiveMessageSenderSpec spec = new MutableReactiveMessageSenderSpec();
		spec.setTopicName("sendNullWithDefaultTopicFails");
		ReactivePulsarSenderFactory<String> senderFactory = new DefaultReactivePulsarSenderFactory<>(client, spec,
				null);
		ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
		assertThatIllegalArgumentException().isThrownBy(() -> pulsarTemplate.send((String) null, Schema.STRING));
	}

	private <T> Message<T> sendAndConsume(Consumer<ReactivePulsarTemplate<T>> sendFunction, String topic,
			Schema<T> schema, T expectedValue, Boolean withDefaultTopic) throws Exception {
		MutableReactiveMessageSenderSpec senderSpec = new MutableReactiveMessageSenderSpec();
		if (withDefaultTopic) {
			senderSpec.setTopicName(topic);
		}
		ReactivePulsarSenderFactory<T> senderFactory = new DefaultReactivePulsarSenderFactory<>(client, senderSpec,
				null);

		ReactivePulsarTemplate<T> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);

		return sendAndConsume(pulsarTemplate, sendFunction, topic, schema, expectedValue);
	}

	private <T> Message<T> sendAndConsume(ReactivePulsarTemplate<T> template,
			Consumer<ReactivePulsarTemplate<T>> sendFunction, String topic, Schema<T> schema, T expectedValue)
			throws Exception {
		try (org.apache.pulsar.client.api.Consumer<T> consumer = client.newConsumer(schema).topic(topic)
				.subscriptionName(topic + "-sub").subscribe()) {
			sendFunction.accept(template);

			Message<T> msg = consumer.receive(3, TimeUnit.SECONDS);
			assertThat(msg).isNotNull();
			assertThat(msg.getValue()).isEqualTo(expectedValue);
			return msg;
		}
	}

	record Foo(String foo, String bar) {
	}

}

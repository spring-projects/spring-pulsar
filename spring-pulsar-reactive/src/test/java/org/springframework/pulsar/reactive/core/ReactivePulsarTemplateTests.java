/*
 * Copyright 2022-present the original author or authors.
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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.JSONSchemaUtil;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordObjectMapper;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.function.ThrowingConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
		await().atMost(Duration.ofSeconds(3))
			.untilAsserted(() -> assertThat(client).extracting("producers")
				.asInstanceOf(InstanceOfAssertFactories.COLLECTION)
				.isEmpty());
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
						(Consumer<ReactivePulsarTemplate<String>>) (
								template) -> template.send("simpleSendWithTopic", message).subscribe(),
						false, message),
				arguments("simpleSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.send(message, Schema.STRING)
							.subscribe(),
						true, message),
				arguments("simpleSendWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
							.send("simpleSendWithTopicAndSchema", message, Schema.STRING)
							.subscribe(),
						false, message),
				arguments("simpleSendNullWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
							.send("simpleSendNullWithTopicAndSchema", (String) null, Schema.STRING)
							.subscribe(),
						false, null),

				arguments("simplePublisherSendWithDefaultTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.send(messagePublisher)
							.subscribe(),
						true, message),
				arguments("simplePublisherSendWithTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
							.send("simplePublisherSendWithTopic", messagePublisher)
							.subscribe(),
						false, message),
				arguments("simplePublisherSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (
								template) -> template.send(messagePublisher, Schema.STRING).subscribe(),
						true, message),
				arguments("simplePublisherSendWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template
							.send("simplePublisherSendWithTopicAndSchema", messagePublisher, Schema.STRING)
							.subscribe(),
						false, message),

				arguments("fluentSendWithDefaultTopic",
						(Consumer<ReactivePulsarTemplate<String>>) (
								template) -> template.newMessage(message).send().subscribe(),
						true, message),
				arguments("fluentSendWithTopic", (Consumer<ReactivePulsarTemplate<String>>) (
						template) -> template.newMessage(message).withTopic("fluentSendWithTopic").send().subscribe(),
						false, message),
				arguments("fluentSendWithDefaultTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (
								template) -> template.newMessage(message).withSchema(Schema.STRING).send().subscribe(),
						true, message),
				arguments("fluentSendNullWithTopicAndSchema",
						(Consumer<ReactivePulsarTemplate<String>>) (template) -> template.newMessage(null)
							.withSchema(Schema.STRING)
							.withTopic("fluentSendNullWithTopicAndSchema")
							.send()
							.subscribe(),
						false, null),
				arguments("fluentPublisherSend", (Consumer<ReactivePulsarTemplate<String>>) (
						template) -> template.newMessages(messagePublisher).send().subscribe(), true, message));
	}

	@Test
	void sendMessageWithMessageCustomizer() throws Exception {
		Consumer<ReactivePulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
			.withMessageCustomizer((mb) -> mb.key("test-key"))
			.send()
			.subscribe();
		Message<?> msg = sendAndConsume(sendFunction, "sendMessageWithMessageCustomizer", Schema.STRING, "test-message",
				true);
		assertThat(msg.getKey()).isEqualTo("test-key");
	}

	@Test
	void sendMessageWithSenderCustomizer() throws Exception {
		Consumer<ReactivePulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
			.withSenderCustomizer((sb) -> sb.producerName("test-producer"))
			.send()
			.subscribe();
		Message<?> msg = sendAndConsume(sendFunction, "sendMessageWithSenderCustomizer", Schema.STRING, "test-message",
				true);
		assertThat(msg.getProducerName()).isEqualTo("test-producer");
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void sendMessageWithTopicInferredByTypeMappings(boolean producerFactoryHasDefaultTopic) throws Exception {
		String topic = "rptt-topicInferred-" + producerFactoryHasDefaultTopic + "-topic";
		ReactivePulsarSenderFactory<Foo> producerFactory = DefaultReactivePulsarSenderFactory.<Foo>builderFor(client)
			.withDefaultTopic(producerFactoryHasDefaultTopic ? "fake-topic" : null)
			.build();
		// Topic mappings allows not specifying the topic when sending (nor having
		// default on producer)
		DefaultTopicResolver topicResolver = new DefaultTopicResolver();
		topicResolver.addCustomTopicMapping(Foo.class, topic);
		ReactivePulsarTemplate<Foo> pulsarTemplate = new ReactivePulsarTemplate<>(producerFactory,
				new DefaultSchemaResolver(), topicResolver);
		Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
		ThrowingConsumer<ReactivePulsarTemplate<Foo>> sendFunction = (
				template) -> template.send(foo, Schema.JSON(Foo.class)).subscribe();
		sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.JSON(Foo.class), foo);
	}

	@Test
	void sendMessageWithoutTopicFails() {
		ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(
				DefaultReactivePulsarSenderFactory.<String>builderFor(client).build());
		assertThatIllegalArgumentException().isThrownBy(() -> pulsarTemplate.send("test-message").subscribe())
			.withMessage("Topic must be specified when no default topic is configured");
	}

	private <T, V> Message<?> sendAndConsume(Consumer<ReactivePulsarTemplate<T>> sendFunction, String topic,
			Schema<V> schema, @Nullable V expectedValue, Boolean withDefaultTopic) throws Exception {
		ReactivePulsarSenderFactory<T> senderFactory = DefaultReactivePulsarSenderFactory.<T>builderFor(client)
			.withDefaultTopic(withDefaultTopic ? topic : null)
			.build();
		ReactivePulsarTemplate<T> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
		return sendAndConsume(pulsarTemplate, sendFunction, topic, schema, expectedValue);
	}

	private <T, V> Message<?> sendAndConsume(ReactivePulsarTemplate<T> template,
			Consumer<ReactivePulsarTemplate<T>> sendFunction, String topic, Schema<V> schema, @Nullable V expectedValue)
			throws Exception {
		try (org.apache.pulsar.client.api.Consumer<V> consumer = client.newConsumer(schema)
			.topic(topic)
			.subscriptionName(topic + "-sub")
			.subscribe()) {
			sendFunction.accept(template);
			Message<?> msg = consumer.receive(3, TimeUnit.SECONDS);
			consumer.acknowledge(msg);
			assertThat(msg).isNotNull();
			assertThat(msg.getValue()).isEqualTo(expectedValue);
			return msg;
		}
	}

	@Nested
	class SendNonPrimitiveSchemaTests {

		@Test
		void withSpecifiedSchema() throws Exception {
			String topic = "rptt-specificSchema-topic";
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<ReactivePulsarTemplate<Foo>> sendFunction = (
					template) -> template.send(foo, Schema.AVRO(Foo.class)).subscribe();
			sendAndConsume(sendFunction, topic, Schema.AVRO(Foo.class), foo, true);
		}

		@Test
		void withSchemaInferredByMessageType() throws Exception {
			String topic = "rptt-nospecificSchema-topic";
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<ReactivePulsarTemplate<Foo>> sendFunction = (template) -> template.send(foo).subscribe();
			sendAndConsume(sendFunction, topic, Schema.JSON(Foo.class), foo, true);
		}

		@Test
		void withSchemaInferredByTypeMappings() throws Exception {
			String topic = "rptt-schemaInferred-topic";
			ReactivePulsarSenderFactory<Foo> producerFactory = DefaultReactivePulsarSenderFactory
				.<Foo>builderFor(client)
				.withDefaultTopic(topic)
				.build();
			// Custom schema resolver allows not specifying the schema when sending
			DefaultSchemaResolver schemaResolver = new DefaultSchemaResolver();
			schemaResolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
			ReactivePulsarTemplate<Foo> pulsarTemplate = new ReactivePulsarTemplate<>(producerFactory, schemaResolver,
					new DefaultTopicResolver());
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<ReactivePulsarTemplate<Foo>> sendFunction = (
					template) -> template.newMessage(foo).send().subscribe();
			sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.JSON(Foo.class), foo);
		}

	}

	@Nested
	class SendNullTests {

		@Test
		void sendNullWithDefaultTopicFails() {
			ReactivePulsarSenderFactory<String> senderFactory = DefaultReactivePulsarSenderFactory
				.<String>builderFor(client)
				.withDefaultConfigCustomizer((builder) -> builder.topic("sendNullWithDefaultTopicFails"))
				.build();
			ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
			assertThatIllegalArgumentException()
				.isThrownBy(() -> pulsarTemplate.send((String) null, Schema.STRING).subscribe())
				.withMessage("Topic must be specified when the message is null");
		}

		@Test
		void sendNullWithoutSchemaFails() {
			ReactivePulsarSenderFactory<String> senderFactory = DefaultReactivePulsarSenderFactory
				.<String>builderFor(client)
				.build();
			ReactivePulsarTemplate<String> pulsarTemplate = new ReactivePulsarTemplate<>(senderFactory);
			assertThatIllegalArgumentException()
				.isThrownBy(() -> pulsarTemplate.send("sendNullWithoutSchemaFails", (String) null, null).subscribe())
				.withMessage("Schema must be specified when the message is null");
		}

	}

	@Nested
	class SendAutoProduceSchemaTests {

		@Test
		void withJsonSchema() throws Exception {
			var topic = "rptt-auto-json-topic";

			// First send to the topic as JSON to establish the schema for the topic
			var userJsonSchema = Schema.JSON(UserRecord.class);
			var user = new UserRecord("Jason", 5150);
			ThrowingConsumer<ReactivePulsarTemplate<UserRecord>> sendAsUserFunction = (
					template) -> template.send(user, userJsonSchema).subscribe();
			sendAndConsume(sendAsUserFunction, topic, userJsonSchema, user, true);

			// Next send another user using byte[] with AUTO_PRODUCE - it should be
			// consumed fine
			var user2 = new UserRecord("Who", 6160);
			var user2Bytes = new ObjectMapper().writeValueAsBytes(user2);
			ThrowingConsumer<ReactivePulsarTemplate<byte[]>> sendAsBytesFunction = (
					template) -> template.send(user2Bytes, Schema.AUTO_PRODUCE_BYTES()).subscribe();
			sendAndConsume(sendAsBytesFunction, topic, userJsonSchema, user2, true);

			// Finally send another user using byte[] with AUTO_PRODUCE w/ invalid payload
			// - it should be rejected
			var bytesSenderFactory = DefaultReactivePulsarSenderFactory.<byte[]>builderFor(client)
				.withDefaultTopic(topic)
				.build();
			var bytesTemplate = new ReactivePulsarTemplate<>(bytesSenderFactory);

			StepVerifier.create(bytesTemplate.send("invalid-payload".getBytes(), Schema.AUTO_PRODUCE_BYTES()))
				.expectError(SchemaSerializationException.class);
		}

	}

	@Nested
	class CustomObjectMapperTests {

		@Test
		void sendWithCustomJsonSchema() throws Exception {
			// Prepare the schema with custom object mapper
			var objectMapper = UserRecordObjectMapper.withSer();
			var schema = JSONSchemaUtil.schemaForTypeWithObjectMapper(UserRecord.class, objectMapper);
			var topic = "rptt-custom-object-mapper-topic";
			var user = new UserRecord("elFoo", 21);
			// serializer adds '-ser' to name and 10 to age
			var expectedUser = new UserRecord("elFoo-ser", 31);
			ThrowingConsumer<ReactivePulsarTemplate<UserRecord>> sendFunction = (
					template) -> template.send(topic, user, schema).subscribe();
			sendAndConsume(sendFunction, topic, schema, expectedUser, false);
		}

	}

	public static class Foo {

		private String foo;

		private String bar;

		Foo() {
		}

		Foo(String foo, String bar) {
			this.foo = foo;
			this.bar = bar;
		}

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Foo foo1 = (Foo) o;
			return foo.equals(foo1.foo) && bar.equals(foo1.bar);
		}

		@Override
		public int hashCode() {
			return Objects.hash(foo, bar);
		}

	}

}

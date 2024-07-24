/*
 * Copyright 2022-2024 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.function.ThrowingConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for {@link PulsarTemplate}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Jonas Geiregat
 */
class PulsarTemplateTests implements PulsarTestContainerSupport {

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
	void sendMessageTest(String testName, ThrowingConsumer<PulsarTemplate<String>> sendFunction,
			Boolean withDefaultTopic, String expectedValue) throws Exception {
		sendAndConsume(sendFunction, testName, Schema.STRING, expectedValue, withDefaultTopic);
	}

	static Stream<Arguments> sendMessageTestProvider() {
		String message = "test-message";

		return Stream.of(
				// Simple send sync
				arguments("simpleSendWithDefaultTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.send(message), true, message),
				arguments("simpleSendWithTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.send("simpleSendWithTopic",
								message),
						false, message),
				arguments("simpleSendWithDefaultTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.send(message, Schema.STRING),
						true, message),
				arguments("simpleSendWithTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template
							.send("simpleSendWithTopicAndSchema", message, Schema.STRING),
						false, message),
				arguments("simpleSendNullWithTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template
							.send("simpleSendNullWithTopicAndSchema", null, Schema.STRING),
						false, null),

				// Simple send async
				arguments("simpleSendAsyncWithDefaultTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.sendAsync(message)
							.get(3, TimeUnit.SECONDS),
						true, message),
				arguments("simpleSendAsyncWithTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template
							.sendAsync("simpleSendAsyncWithTopic", message)
							.get(3, TimeUnit.SECONDS),
						false, message),
				arguments("simpleSendAsyncWithDefaultTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (
								template) -> template.sendAsync(message, Schema.STRING).get(3, TimeUnit.SECONDS),
						true, message),
				arguments("simpleSendAsyncWithTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template
							.sendAsync("simpleSendAsyncWithTopicAndSchema", message, Schema.STRING)
							.get(3, TimeUnit.SECONDS),
						false, message),
				arguments("simpleSendAsyncNullWithTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template
							.sendAsync("simpleSendAsyncNullWithTopicAndSchema", null, Schema.STRING)
							.get(3, TimeUnit.SECONDS),
						false, null),

				// Fluent send
				arguments("fluentSendWithDefaultTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.newMessage(message).send(),
						true, message),
				arguments("fluentSendWithTopic",
						(ThrowingConsumer<PulsarTemplate<String>>) (
								template) -> template.newMessage(message).withTopic("fluentSendWithTopic").send(),
						false, message),
				arguments("fluentSendWithDefaultTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (
								template) -> template.newMessage(message).withSchema(Schema.STRING).send(),
						true, message),
				arguments("fluentSendNullWithTopicAndSchema",
						(ThrowingConsumer<PulsarTemplate<String>>) (template) -> template.newMessage(null)
							.withSchema(Schema.STRING)
							.withTopic("fluentSendNullWithTopicAndSchema")
							.send(),
						false, null),
				arguments("fluentSendAsync",
						(ThrowingConsumer<PulsarTemplate<String>>) (
								template) -> template.newMessage(message).sendAsync().get(3, TimeUnit.SECONDS),
						true, message)

		);
	}

	@Test
	void sendMessageWithMessageCustomizer() throws Exception {
		ThrowingConsumer<PulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
			.withMessageCustomizer((mb) -> mb.key("test-key"))
			.send();
		Message<?> msg = sendAndConsume(sendFunction, "sendMessageWithMessageCustomizer", Schema.STRING, "test-message",
				true);
		assertThat(msg.getKey()).isEqualTo("test-key");
	}

	@Test
	void sendMessageWithSenderCustomizer() throws Exception {
		ThrowingConsumer<PulsarTemplate<String>> sendFunction = (template) -> template.newMessage("test-message")
			.withProducerCustomizer((sb) -> sb.producerName("test-producer"))
			.send();
		Message<?> msg = sendAndConsume(sendFunction, "sendMessageWithSenderCustomizer", Schema.STRING, "test-message",
				true);
		assertThat(msg.getProducerName()).isEqualTo("test-producer");
	}

	@Test
	@SuppressWarnings("unchecked")
	void sendMessageWithEncryptionKeys() throws Exception {
		String topic = "ptt-encryptionKeys-topic";
		PulsarProducerFactory<String> producerFactory = mock(PulsarProducerFactory.class);
		when(producerFactory.createProducer(Schema.STRING, topic, Set.of("key"), new ArrayList<>()))
			.thenReturn(client.newProducer(Schema.STRING).topic(topic).create());
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory);
		pulsarTemplate.newMessage("msg").withTopic(topic).withEncryptionKeys(Set.of("key")).send();
		verify(producerFactory).createProducer(Schema.STRING, topic, Set.of("key"), new ArrayList<>());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("interceptorInvocationTestProvider")
	void interceptorInvocationTest(String topic, List<ProducerInterceptor> interceptors) {
		PulsarProducerFactory<String> producerFactory = new DefaultPulsarProducerFactory<>(client, topic);
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(producerFactory, interceptors);
		pulsarTemplate.send("test-interceptor");
		for (ProducerInterceptor interceptor : interceptors) {
			verify(interceptor, atLeastOnce()).eligible(any(Message.class));
		}
	}

	private static Stream<Arguments> interceptorInvocationTestProvider() {
		return Stream.of(
				arguments(Named.of("singleInterceptor", "iit-topic-1"),
						Collections.singletonList(mock(ProducerInterceptor.class))),
				arguments(Named.of("multipleInterceptors", "iit-topic-2"),
						List.of(mock(ProducerInterceptor.class), mock(ProducerInterceptor.class))));
	}

	@Test
	void interceptorUsedAsCacheKeyProperly() {
		var producerFactory = new CachingPulsarProducerFactory<String>(client, null, null, new DefaultTopicResolver(),
				Duration.ofSeconds(10L), 10L, 10);
		try {
			var interceptors = List.of(mock(ProducerInterceptor.class));
			var pulsarTemplate = new PulsarTemplate<>(producerFactory, interceptors);
			assertCacheSize(producerFactory, 0);
			IntStream.range(0, 3).forEach((i) -> {
				pulsarTemplate.send("test-intercept-topic", "test-interceptor-" + i);
				assertCacheSize(producerFactory, 1);
			});
			assertCacheSize(producerFactory, 1);
		}
		finally {
			// The CPPF returns producers that do not actually close when the template
			// calls close on them - destroy does close the producers though
			if (producerFactory != null) {
				producerFactory.destroy();
			}
		}
	}

	private <T> void assertCacheSize(CachingPulsarProducerFactory<T> producerFactory, int expectedSize) {
		assertThat(producerFactory).extracting("producerCache.cache.cache")
			.asInstanceOf(InstanceOfAssertFactories.MAP)
			.hasSize(expectedSize);
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void sendMessageWithTopicInferredByTypeMappings(boolean producerFactoryHasDefaultTopic) throws Exception {
		String topic = "ptt-topicInferred-" + producerFactoryHasDefaultTopic + "-topic";
		PulsarProducerFactory<Foo> producerFactory = new DefaultPulsarProducerFactory<>(client,
				producerFactoryHasDefaultTopic ? "fake-topic" : null);
		// Topic mappings allows not specifying the topic when sending (nor having
		// default on producer)
		DefaultTopicResolver topicResolver = new DefaultTopicResolver();
		topicResolver.addCustomTopicMapping(Foo.class, topic);
		PulsarTemplate<Foo> pulsarTemplate = new PulsarTemplate<>(producerFactory, Collections.emptyList(),
				new DefaultSchemaResolver(), topicResolver, false);
		Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
		ThrowingConsumer<PulsarTemplate<Foo>> sendFunction = (template) -> template.send(foo, Schema.JSON(Foo.class));
		sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.JSON(Foo.class), foo);
	}

	@Test
	void sendMessageWithoutTopicFails() {
		PulsarProducerFactory<String> senderFactory = new DefaultPulsarProducerFactory<>(client);
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(senderFactory);
		assertThatIllegalArgumentException().isThrownBy(() -> pulsarTemplate.send("test-message"))
			.withMessage("Topic must be specified when no default topic is configured");
	}

	private <T, V> Message<?> sendAndConsume(ThrowingConsumer<PulsarTemplate<T>> sendFunction, String topic,
			Schema<V> consumingSchema, V expectedValue, Boolean withDefaultTopic) throws Exception {
		PulsarProducerFactory<T> senderFactory = new DefaultPulsarProducerFactory<>(client,
				withDefaultTopic ? topic : null);
		PulsarTemplate<T> pulsarTemplate = new PulsarTemplate<>(senderFactory);
		return sendAndConsume(pulsarTemplate, sendFunction, topic, consumingSchema, expectedValue);
	}

	private <T, V> Message<?> sendAndConsume(PulsarTemplate<T> template,
			ThrowingConsumer<PulsarTemplate<T>> sendFunction, String topic, Schema<V> schema, V expectedValue)
			throws Exception {
		try (org.apache.pulsar.client.api.Consumer<?> consumer = client.newConsumer(schema)
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
			String topic = "ptt-specificSchema-topic";
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<PulsarTemplate<Foo>> sendFunction = (template) -> template.send(foo,
					Schema.AVRO(Foo.class));
			sendAndConsume(sendFunction, topic, Schema.AVRO(Foo.class), foo, true);
		}

		@Test
		void withSchemaInferredByMessageType() throws Exception {
			String topic = "ptt-nospecificSchema-topic";
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<PulsarTemplate<Foo>> sendFunction = (template) -> template.send(foo);
			sendAndConsume(sendFunction, topic, Schema.JSON(Foo.class), foo, true);
		}

		@Test
		void withSchemaInferredByTypeMappings() throws Exception {
			String topic = "ptt-schemaInferred-topic";
			PulsarProducerFactory<Foo> producerFactory = new DefaultPulsarProducerFactory<>(client, topic);
			// Custom schema resolver allows not specifying the schema when sending
			DefaultSchemaResolver schemaResolver = new DefaultSchemaResolver();
			schemaResolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
			PulsarTemplate<Foo> pulsarTemplate = new PulsarTemplate<>(producerFactory, Collections.emptyList(),
					schemaResolver, new DefaultTopicResolver(), false);
			Foo foo = new Foo("Foo-" + UUID.randomUUID(), "Bar-" + UUID.randomUUID());
			ThrowingConsumer<PulsarTemplate<Foo>> sendFunction = (template) -> template.newMessage(foo).send();
			sendAndConsume(pulsarTemplate, sendFunction, topic, Schema.JSON(Foo.class), foo);
		}

	}

	@Nested
	class SendNullTests {

		@Test
		void sendNullWithDefaultTopicFails() {
			PulsarProducerFactory<String> senderFactory = new DefaultPulsarProducerFactory<>(client,
					"sendNullWithDefaultTopicFails");
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(senderFactory);
			assertThatIllegalArgumentException().isThrownBy(() -> pulsarTemplate.send(null, Schema.STRING))
				.withMessage("Topic must be specified when the message is null");
		}

		@Test
		void sendNullWithoutSchemaFails() {
			PulsarProducerFactory<Object> senderFactory = new DefaultPulsarProducerFactory<>(client);
			PulsarTemplate<Object> pulsarTemplate = new PulsarTemplate<>(senderFactory);
			assertThatIllegalArgumentException()
				.isThrownBy(() -> pulsarTemplate.send("sendNullWithoutSchemaFails", null, null))
				.withMessage("Schema must be specified when the message is null");
		}

	}

	@Nested
	class SendAutoProduceSchemaTests {

		@Test
		void withJsonSchema() throws Exception {
			var topic = "ptt-auto-json-topic";

			// First send to the topic as JSON to establish the schema for the topic
			var userJsonSchema = Schema.JSON(UserRecord.class);
			var user = new UserRecord("Jason", 5150);
			ThrowingConsumer<PulsarTemplate<UserRecord>> sendAsUserFunction = (template) -> template.send(user,
					userJsonSchema);
			sendAndConsume(sendAsUserFunction, topic, userJsonSchema, user, true);

			// Next send another user using byte[] with AUTO_PRODUCE - it should be
			// consumed fine
			var user2 = new UserRecord("Who", 6160);
			var user2Bytes = new ObjectMapper().writeValueAsBytes(user2);
			ThrowingConsumer<PulsarTemplate<byte[]>> sendAsBytesFunction = (template) -> template.send(user2Bytes,
					Schema.AUTO_PRODUCE_BYTES());
			sendAndConsume(sendAsBytesFunction, topic, userJsonSchema, user2, true);

			// Finally send another user using byte[] with AUTO_PRODUCE w/ invalid payload
			// - it should be rejected
			var bytesProducerFactory = new DefaultPulsarProducerFactory<byte[]>(client, topic);
			var bytesTemplate = new PulsarTemplate<>(bytesProducerFactory);
			assertThatExceptionOfType(SchemaSerializationException.class)
				.isThrownBy(() -> bytesTemplate.send("invalid-payload".getBytes(), Schema.AUTO_PRODUCE_BYTES()));
		}

	}

	@Nested
	class PulsarTemplateCustomizerTests {

		@Test
		void whenSingleCustomizerAvailableThenItIsApplied() {
			var template = mock(PulsarTemplate.class);
			var txnProps = mock(TransactionProperties.class);
			when(template.transactions()).thenReturn(txnProps);
			PulsarTemplateCustomizer<?> customizer = (t) -> t.transactions().setTimeout(Duration.ofSeconds(45));
			try (var appContext = new AnnotationConfigApplicationContext()) {
				appContext.registerBean(PulsarTemplate.class, () -> template);
				appContext.registerBean(PulsarTemplateCustomizer.class, () -> customizer);
				appContext.register(PulsarTemplateCustomizerTestsConfig.class);
				appContext.refresh();
				verify(txnProps).setTimeout(Duration.ofSeconds(45));
			}
		}

		@Test
		void whenMultipleCustomizersAvailableThenNoneAreApplied() {
			var template = mock(PulsarTemplate.class);
			var txnProps = mock(TransactionProperties.class);
			when(template.transactions()).thenReturn(txnProps);
			PulsarTemplateCustomizer<?> customizer1 = (t) -> t.transactions().setTimeout(Duration.ofSeconds(30));
			PulsarTemplateCustomizer<?> customizer2 = (t) -> t.transactions().setTimeout(Duration.ofSeconds(45));
			try (var appContext = new AnnotationConfigApplicationContext()) {
				appContext.registerBean(PulsarTemplate.class, () -> template);
				appContext.registerBean("customizer1", PulsarTemplateCustomizer.class, () -> customizer1);
				appContext.registerBean("customizer2", PulsarTemplateCustomizer.class, () -> customizer2);
				appContext.register(PulsarTemplateCustomizerTestsConfig.class);
				appContext.refresh();
				verify(txnProps, never()).setTimeout(any(Duration.class));
			}
		}

		@Test
		void whenNoCustomizersAvaiableThenContextStartsWithoutFailure() {
			var template = mock(PulsarTemplate.class);
			try (var appContext = new AnnotationConfigApplicationContext()) {
				appContext.registerBean(PulsarTemplate.class, () -> template);
				appContext.register(PulsarTemplateCustomizerTestsConfig.class);
				appContext.refresh();
			}
		}

		@Configuration(proxyBeanMethods = false)
		@EnablePulsar
		static class PulsarTemplateCustomizerTestsConfig {

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

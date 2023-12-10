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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.BatchComplexPayload.BatchComplexPayloadConfig;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.BatchPrimitivePayload.BatchPrimitivePayloadConfig;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.PulsarMessagePayload.PulsarMessagePayloadConfig;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.SingleComplexPayload.SingleComplexPayloadConfig;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.SinglePrimitivePayload.SinglePrimitivePayloadConfig;
import org.springframework.pulsar.listener.PulsarListenerTombstoneTests.SpringMessagePayload.SpringMessagePayloadConfig;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.support.PulsarNull;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests consuming {@link PulsarNull tombstone} records in
 * {@link PulsarListener @PulsarListener}.
 *
 * @author Chris Bono
 */
class PulsarListenerTombstoneTests extends PulsarListenerTestsBase {

	static <T> void sendTestMessages(PulsarTemplate<T> pulsarTemplate, String topic, Schema<T> schema,
			Function<String, T> payloadFactory) throws PulsarClientException {
		pulsarTemplate.newMessage(payloadFactory.apply("foo"))
			.withTopic(topic)
			.withMessageCustomizer((mb) -> mb.key("key:foo"))
			.send();
		pulsarTemplate.newMessage(null)
			.withTopic(topic)
			.withSchema(schema)
			.withMessageCustomizer((mb) -> mb.key("key:null"))
			.send();
		pulsarTemplate.newMessage(payloadFactory.apply("bar"))
			.withTopic(topic)
			.withMessageCustomizer((mb) -> mb.key("key:bar"))
			.send();
	}

	static <T> void assertMessagesReceivedWithHeaders(List<ReceivedMessage<T>> receivedMessages,
			Function<String, T> payloadFactory) {
		assertThat(receivedMessages).containsExactly(new ReceivedMessage<>(payloadFactory.apply("foo"), "key:foo"),
				new ReceivedMessage<>(null, "key:null"), new ReceivedMessage<>(payloadFactory.apply("bar"), "key:bar"));
	}

	static <T> void assertMessagesReceivedWithoutHeaders(List<ReceivedMessage<T>> receivedMessages,
			Function<String, T> payloadFactory) {
		assertThat(receivedMessages).containsExactly(new ReceivedMessage<>(payloadFactory.apply("foo"), null),
				new ReceivedMessage<>(null, null), new ReceivedMessage<>(payloadFactory.apply("bar"), null));
	}

	@Nested
	@ContextConfiguration(classes = PulsarMessagePayloadConfig.class)
	class PulsarMessagePayload {

		private static final String TOPIC = "pltt-pulsar-msg-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Function.identity());
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class PulsarMessagePayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.STRING,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(org.apache.pulsar.client.api.Message<String> msg,
					@Header(PulsarHeaders.KEY) String key) {
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg.getValue(), key));
				latchWithHeaders.countDown();
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithoutHeaders(org.apache.pulsar.client.api.Message<String> msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg.getValue(), null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SpringMessagePayloadConfig.class)
	class SpringMessagePayload {

		private static final String TOPIC = "pltt-spring-msg-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Function.identity());
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class SpringMessagePayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.STRING,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(Message<?> msg, @Header(PulsarHeaders.KEY) String key) {
				var payload = (msg.getPayload() != PulsarNull.INSTANCE) ? msg.getPayload().toString() : null;
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(payload, key));
				latchWithHeaders.countDown();
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithoutHeaders(Message<Object> msg) {
				var payload = (msg.getPayload() != PulsarNull.INSTANCE) ? msg.getPayload().toString() : null;
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(payload, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SinglePrimitivePayloadConfig.class)
	class SinglePrimitivePayload {

		private static final String TOPIC = "pltt-single-primitive-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Function.identity());
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class SinglePrimitivePayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.STRING,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(@Payload(required = false) String msg,
					@Header(PulsarHeaders.KEY) String key) {
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg, key));
				latchWithHeaders.countDown();
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithoutHeaders(@Payload(required = false) String msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = BatchPrimitivePayloadConfig.class)
	class BatchPrimitivePayload {

		private static final String TOPIC = "pltt-multi-primitive-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Function.identity());
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class BatchPrimitivePayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.STRING,
					batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(List<String> msgs, @Header(PulsarHeaders.KEY) List<String> keys) {
				for (int i = 0; i < msgs.size(); i++) {
					receivedMessagesWithHeaders.add(new ReceivedMessage<>(msgs.get(i), keys.get(i)));
					latchWithHeaders.countDown();
				}
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, batch = true,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenMultipleNoHeaders(List<String> msgs) {
				for (int i = 0; i < msgs.size(); i++) {
					receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msgs.get(i), null));
					latchWithoutHeaders.countDown();
				}
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SingleComplexPayloadConfig.class)
	class SingleComplexPayload {

		private static final String TOPIC = "pltt-single-complex-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<Foo>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<Foo>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Foo>(pulsarClient);
			var fooPulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			sendTestMessages(fooPulsarTemplate, TOPIC, Schema.JSON(Foo.class), Foo::new);
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Foo::new);
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Foo::new);
		}

		@Configuration(proxyBeanMethods = false)
		static class SingleComplexPayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.JSON,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(@Payload(required = false) Foo msg, @Header(PulsarHeaders.KEY) String key) {
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg, key));
				latchWithHeaders.countDown();
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers", schemaType = SchemaType.JSON,
					properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithoutHeaders(@Payload(required = false) Foo msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = BatchComplexPayloadConfig.class)
	class BatchComplexPayload {

		private static final String TOPIC = "pltt-multi-complex-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<Foo>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<Foo>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Foo>(pulsarClient);
			var fooPulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			sendTestMessages(fooPulsarTemplate, TOPIC, Schema.JSON(Foo.class), Foo::new);
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Foo::new);
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Foo::new);
		}

		@Configuration(proxyBeanMethods = false)
		static class BatchComplexPayloadConfig {

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers", schemaType = SchemaType.JSON,
					batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenWithHeaders(List<Foo> msgs, @Header(PulsarHeaders.KEY) List<String> keys) {
				for (int i = 0; i < msgs.size(); i++) {
					receivedMessagesWithHeaders.add(new ReceivedMessage<>(msgs.get(i), keys.get(i)));
					latchWithHeaders.countDown();
				}
			}

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers", schemaType = SchemaType.JSON,
					batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			public void listenMultipleNoHeaders(List<Foo> msgs) {
				for (int i = 0; i < msgs.size(); i++) {
					receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msgs.get(i), null));
					latchWithoutHeaders.countDown();
				}
			}

		}

	}

	record Foo(String value) {
	}

	record ReceivedMessage<T>(T payload, String keyHeader) {
	}

}

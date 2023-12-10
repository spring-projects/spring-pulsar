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

package org.springframework.pulsar.reader;

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
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.reader.PulsarReaderBasicTests.PulsarMessagePayload.PulsarMessagePayloadConfig;
import org.springframework.pulsar.reader.PulsarReaderBasicTests.SingleComplexPayload.SingleComplexPayloadConfig;
import org.springframework.pulsar.reader.PulsarReaderBasicTests.SinglePrimitivePayload.SinglePrimitivePayloadConfig;
import org.springframework.pulsar.reader.PulsarReaderBasicTests.SpringMessagePayload.SpringMessagePayloadConfig;
import org.springframework.pulsar.support.PulsarNull;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests consuming records (including {@link PulsarNull tombstones}) in
 * {@link PulsarReader @PulsarReader}.
 *
 * @author Chris Bono
 */
class PulsarReaderBasicTests extends PulsarReaderTestsBase {

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

	static <T> void assertMessagesReceivedWithoutHeaders(List<ReceivedMessage<T>> receivedMessages,
			Function<String, T> payloadFactory) {
		assertThat(receivedMessages).containsExactly(new ReceivedMessage<>(payloadFactory.apply("foo"), null),
				new ReceivedMessage<>(null, null), new ReceivedMessage<>(payloadFactory.apply("bar"), null));
	}

	@Nested
	@ContextConfiguration(classes = PulsarMessagePayloadConfig.class)
	class PulsarMessagePayload {

		private static final String TOPIC = "prbt-pulsar-msg-topic";

		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class PulsarMessagePayloadConfig {

			@PulsarReader(topics = TOPIC, schemaType = SchemaType.STRING, startMessageId = "earliest")
			public void listenWithoutHeaders(org.apache.pulsar.client.api.Message<String> msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg.getValue(), null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SpringMessagePayloadConfig.class)
	class SpringMessagePayload {

		private static final String TOPIC = "prbt-spring-msg-topic";

		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());

		}

		@Configuration(proxyBeanMethods = false)
		static class SpringMessagePayloadConfig {

			@PulsarReader(topics = TOPIC, schemaType = SchemaType.STRING, startMessageId = "earliest")
			public void listenWithoutHeaders(Message<Object> msg) {
				var payload = (msg.getPayload() != PulsarNull.INSTANCE) ? msg.getPayload().toString() : null;
				assertThat(msg.getHeaders()).isNotEmpty();
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(payload, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SinglePrimitivePayloadConfig.class)
	class SinglePrimitivePayload {

		private static final String TOPIC = "prbt-single-primitive-topic";

		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class SinglePrimitivePayloadConfig {

			@PulsarReader(topics = TOPIC, schemaType = SchemaType.STRING, startMessageId = "earliest")
			public void listenWithoutHeaders(@Payload(required = false) String msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SingleComplexPayloadConfig.class)
	class SingleComplexPayload {

		private static final String TOPIC = "prbt-single-complex-topic";

		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<Foo>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Foo>(pulsarClient);
			var fooPulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			sendTestMessages(fooPulsarTemplate, TOPIC, Schema.JSON(Foo.class), Foo::new);
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Foo::new);
		}

		@Configuration(proxyBeanMethods = false)
		static class SingleComplexPayloadConfig {

			@PulsarReader(topics = TOPIC, schemaType = SchemaType.JSON, startMessageId = "earliest")
			public void listenWithoutHeaders(@Payload(required = false) Foo msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				latchWithoutHeaders.countDown();
			}

		}

	}

	record Foo(String value) {
	}

	record ReceivedMessage<T>(T payload, String keyHeader) {
	}

}

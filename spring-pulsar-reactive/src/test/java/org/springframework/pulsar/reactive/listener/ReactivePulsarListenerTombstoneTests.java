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

package org.springframework.pulsar.reactive.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.PulsarMessagePayload.PulsarMessagePayloadConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.SingleComplexPayload.SingleComplexPayloadConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.SinglePrimitivePayload.SinglePrimitivePayloadConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.SpringMessagePayload.SpringMessagePayloadConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.StreamingPulsarMessagePayload.StreamingPulsarMessagePayloadConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTombstoneTests.StreamingSpringMessagePayload.StreamingSpringMessagePayloadConfig;
import org.springframework.pulsar.reactive.support.MessageUtils;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.support.PulsarNull;
import org.springframework.test.context.ContextConfiguration;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests consuming records (including {@link PulsarNull tombstones}) in
 * {@link ReactivePulsarListener @ReactivePulsarListener}.
 *
 * @author Chris Bono
 */
class ReactivePulsarListenerTombstoneTests extends ReactivePulsarListenerTestsBase {

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

		private static final String TOPIC = "rpltt-pulsar-msg-topic";

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

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithHeaders(org.apache.pulsar.client.api.Message<String> msg,
					@Header(PulsarHeaders.KEY) String key) {
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg.getValue(), key));
				latchWithHeaders.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithoutHeaders(org.apache.pulsar.client.api.Message<String> msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg.getValue(), null));
				latchWithoutHeaders.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SpringMessagePayloadConfig.class)
	class SpringMessagePayload {

		private static final String TOPIC = "rpltt-spring-msg-topic";

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

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithHeaders(Message<?> msg, @Header(PulsarHeaders.KEY) String key) {
				var payload = (msg.getPayload() != PulsarNull.INSTANCE) ? msg.getPayload().toString() : null;
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(payload, key));
				latchWithHeaders.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithoutHeaders(Message<Object> msg) {
				var payload = (msg.getPayload() != PulsarNull.INSTANCE) ? msg.getPayload().toString() : null;
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(payload, null));
				latchWithoutHeaders.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SinglePrimitivePayloadConfig.class)
	class SinglePrimitivePayload {

		private static final String TOPIC = "rpltt-single-primitive-topic";

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

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithHeaders(@Payload(required = false) String msg, @Header(PulsarHeaders.KEY) String key) {
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg, key));
				latchWithHeaders.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.STRING, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithoutHeaders(@Payload(required = false) String msg) {
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				latchWithoutHeaders.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SingleComplexPayloadConfig.class)
	class SingleComplexPayload {

		private final LogAccessor logger = new LogAccessor(this.getClass());

		private static final String TOPIC = "rpltt-single-complex-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<Foo>> receivedMessagesWithHeaders = new ArrayList<>();
		static List<ReceivedMessage<Foo>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Foo>(pulsarClient);
			var fooPulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			sendTestMessages(fooPulsarTemplate, TOPIC, Schema.JSON(Foo.class), Foo::new);
			assertThat(latchWithHeaders.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(latchWithoutHeaders.await(10, TimeUnit.SECONDS)).isTrue();

			// Temporary log to analyze CI failures due to flaky test, one such failure
			// case:
			// https://github.com/spring-projects/spring-pulsar/actions/runs/7598761030/job/20695067626
			for (ReceivedMessage<Foo> message : receivedMessagesWithHeaders) {
				logger.info(message.toString());
			}
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Foo::new);
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Foo::new);
		}

		@Configuration(proxyBeanMethods = false)
		static class SingleComplexPayloadConfig {

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-headers",
					schemaType = SchemaType.JSON, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithHeaders(@Payload(required = false) Foo msg, @Header(PulsarHeaders.KEY) String key) {
				latchWithHeaders.countDown();
				receivedMessagesWithHeaders.add(new ReceivedMessage<>(msg, key));
				return Mono.empty();
			}

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub-no-headers",
					schemaType = SchemaType.JSON, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenWithoutHeaders(@Payload(required = false) Foo msg) {
				latchWithoutHeaders.countDown();
				receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(msg, null));
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = StreamingPulsarMessagePayloadConfig.class)
	class StreamingPulsarMessagePayload {

		private static final String TOPIC = "rpltt-multi-pulsar-msg-topic";

		static CountDownLatch latchWithoutHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<String>> receivedMessagesWithoutHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			sendTestMessages(pulsarTemplate, TOPIC, Schema.STRING, Function.identity());
			assertThat(latchWithoutHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithoutHeaders(receivedMessagesWithoutHeaders, Function.identity());
		}

		@Configuration(proxyBeanMethods = false)
		static class StreamingPulsarMessagePayloadConfig {

			@ReactivePulsarListener(topics = TOPIC, stream = true, schemaType = SchemaType.STRING,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listenWithoutHeaders(
					Flux<org.apache.pulsar.client.api.Message<String>> messages) {
				return messages.doOnNext(m -> {
					receivedMessagesWithoutHeaders.add(new ReceivedMessage<>(m.getValue(), null));
					latchWithoutHeaders.countDown();
				}).map(MessageResult::acknowledge);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = StreamingSpringMessagePayloadConfig.class)
	class StreamingSpringMessagePayload {

		private static final String TOPIC = "rpltt-multi-spring-msg-topic";

		static CountDownLatch latchWithHeaders = new CountDownLatch(3);
		static List<ReceivedMessage<Object>> receivedMessagesWithHeaders = new ArrayList<>();

		@Test
		void shouldReceiveMessagesWithTombstone() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Foo>(pulsarClient);
			var fooPulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			sendTestMessages(fooPulsarTemplate, TOPIC, Schema.JSON(Foo.class), Foo::new);
			assertThat(latchWithHeaders.await(5, TimeUnit.SECONDS)).isTrue();
			assertMessagesReceivedWithHeaders(receivedMessagesWithHeaders, Foo::new);
		}

		@SuppressWarnings("rawtypes")
		@Configuration(proxyBeanMethods = false)
		static class StreamingSpringMessagePayloadConfig {

			@ReactivePulsarListener(topics = TOPIC, stream = true, schemaType = SchemaType.JSON,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listenWithHeaders(Flux<Message<Object>> messages) {
				return messages.doOnNext(m -> {
					Object payload = m.getPayload();
					if (payload == PulsarNull.INSTANCE) {
						payload = null;
					}
					else if (payload instanceof Map payloadFields) {
						payload = new Foo((String) payloadFields.get("value"));
					}
					var keyHeader = (String) m.getHeaders().get(PulsarHeaders.KEY);
					receivedMessagesWithHeaders.add(new ReceivedMessage<>(payload, keyHeader));
					latchWithHeaders.countDown();
				}).map(MessageUtils::acknowledge);
			}

		}

	}

	record Foo(String value) {
	}

	record ReceivedMessage<T>(T payload, String keyHeader) {
	}

}

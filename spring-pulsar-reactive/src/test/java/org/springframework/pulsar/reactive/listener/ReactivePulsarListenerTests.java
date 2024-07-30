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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.EnableReactivePulsar;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.BasicListenersTestCases.BasicListenersTestCasesConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.PulsarHeadersCustomObjectMapperTest.PulsarHeadersCustomObjectMapperTestConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.PulsarHeadersTest.PulsarListenerWithHeadersConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.StreamingListenerTestCases.StreamingListenerTestCasesConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.SubscriptionTypeTests.WithDefaultType.WithDefaultTypeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.SubscriptionTypeTests.WithSpecificTypes.WithSpecificTypesConfig;
import org.springframework.pulsar.reactive.support.MessageUtils;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper;
import org.springframework.pulsar.test.model.UserPojo;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordDeserializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link ReactivePulsarListener} annotation.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class ReactivePulsarListenerTests extends ReactivePulsarListenerTestsBase {

	@Nested
	@ContextConfiguration(classes = BasicListenersTestCasesConfig.class)
	class BasicListenersTestCases {

		static CountDownLatch latch1 = new CountDownLatch(1);
		static CountDownLatch latch2 = new CountDownLatch(1);
		static CountDownLatch latch3 = new CountDownLatch(3);

		@Autowired
		ReactivePulsarListenerEndpointRegistry<String> registry;

		@Test
		void testPulsarListener() throws Exception {
			ReactivePulsarContainerProperties<String> pulsarContainerProperties = registry.getListenerContainer("id-1")
				.getContainerProperties();
			assertThat(pulsarContainerProperties.getTopics()).containsExactly("topic-1");
			assertThat(pulsarContainerProperties.getSubscriptionName()).isEqualTo("subscription-1");
			pulsarTemplate.send("topic-1", "hello foo");
			assertThat(latch1.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void testPulsarListenerWithConsumerCustomizer() throws Exception {
			pulsarTemplate.send("topic-2", "hello foo");
			assertThat(latch2.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void testPulsarListenerWithTopicsPattern() throws Exception {
			ReactivePulsarContainerProperties<String> containerProperties = registry.getListenerContainer("id-3")
				.getContainerProperties();
			assertThat(containerProperties.getTopicsPattern().toString())
				.isEqualTo("persistent://public/default/pattern.*");

			// Let things setup before firing the messages
			Thread.sleep(2000);

			pulsarTemplate.send("persistent://public/default/pattern-1", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-2", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-3", "hello baz");

			assertThat(latch3.await(15, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class BasicListenersTestCasesConfig {

			@ReactivePulsarListener(id = "id-1", topics = "topic-1", subscriptionName = "subscription-1",
					consumerCustomizer = "listen1Customizer")
			Mono<Void> listen1(String ignored) {
				latch1.countDown();
				return Mono.empty();
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> listen1Customizer() {
				return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

			@ReactivePulsarListener(consumerCustomizer = "listen2Customizer")
			Mono<Void> listen2(String ignored) {
				latch2.countDown();
				return Mono.empty();
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> listen2Customizer() {
				return b -> b.topics(List.of("topic-2"))
					.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

			@ReactivePulsarListener(id = "id-3", topicPattern = "persistent://public/default/pattern.*",
					subscriptionName = "subscription-3", consumerCustomizer = "listen3Customizer")
			Mono<Void> listen3(String ignored) {
				latch3.countDown();
				return Mono.empty();
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> listen3Customizer() {
				return b -> b.topicsPatternAutoDiscoveryPeriod(Duration.ofSeconds(5))
					.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = StreamingListenerTestCasesConfig.class)
	class StreamingListenerTestCases {

		static CountDownLatch latch1 = new CountDownLatch(10);
		static CountDownLatch latch2 = new CountDownLatch(10);

		@Test
		void testPulsarListenerStreaming() throws Exception {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("streaming-1", "hello foo");
			}
			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void testPulsarListenerStreamingSpringMessage() throws Exception {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("streaming-2", "hello foo");
			}
			assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class StreamingListenerTestCasesConfig {

			@ReactivePulsarListener(topics = "streaming-1", stream = true,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listen1(Flux<Message<String>> messages) {
				return messages.doOnNext(m -> latch1.countDown()).map(MessageResult::acknowledge);
			}

			@ReactivePulsarListener(topics = "streaming-2", stream = true,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listen2(Flux<org.springframework.messaging.Message<String>> messages) {
				return messages.doOnNext(m -> latch2.countDown()).map(MessageUtils::acknowledge);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = DeadLetterPolicyTest.DeadLetterPolicyConfig.class)
	class DeadLetterPolicyTest {

		private static CountDownLatch latch = new CountDownLatch(2);

		private static CountDownLatch dlqLatch = new CountDownLatch(1);

		@Test
		void pulsarListenerWithDeadLetterPolicy() throws Exception {
			pulsarTemplate.send("dlpt-topic-1", "hello");
			assertThat(dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class DeadLetterPolicyConfig {

			@ReactivePulsarListener(id = "deadLetterPolicyListener", subscriptionName = "deadLetterPolicySubscription",
					topics = "dlpt-topic-1", deadLetterPolicy = "deadLetterPolicy",
					consumerCustomizer = "consumerCustomizer", subscriptionType = SubscriptionType.Shared)
			Mono<Void> listen(String msg) {
				latch.countDown();
				return Mono.error(new RuntimeException("fail " + msg));
			}

			@ReactivePulsarListener(id = "dlqListener", topics = "dlpt-dlq-topic",
					consumerCustomizer = "consumerCustomizer")
			Mono<Void> listenDlq(String msg) {
				dlqLatch.countDown();
				return Mono.empty();
			}

			@Bean
			DeadLetterPolicy deadLetterPolicy() {
				return DeadLetterPolicy.builder().maxRedeliverCount(1).deadLetterTopic("dlpt-dlq-topic").build();
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
				return b -> b.negativeAckRedeliveryDelay(Duration.ofSeconds(1))
					.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SchemaTestCases.SchemaTestConfig.class)
	class SchemaTestCases {

		static CountDownLatch jsonLatch = new CountDownLatch(3);
		static CountDownLatch avroLatch = new CountDownLatch(3);
		static CountDownLatch keyvalueLatch = new CountDownLatch(3);
		static CountDownLatch protobufLatch = new CountDownLatch(3);

		@Test
		void jsonSchema() throws Exception {
			PulsarProducerFactory<UserPojo> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<UserPojo> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("json-topic", new UserPojo("Jason", i), JSONSchema.of(UserPojo.class));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<UserPojo> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<UserPojo> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("avro-topic", new UserPojo("Avi", i), AvroSchema.of(UserPojo.class));
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, Integer>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient);
			PulsarTemplate<KeyValue<String, Integer>> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<KeyValue<String, Integer>> kvSchema = Schema.KeyValue(Schema.STRING, Schema.INT32,
					KeyValueEncodingType.INLINE);
			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-topic", new KeyValue<>("Kevin", i), kvSchema);
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient);
			PulsarTemplate<Proto.Person> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("protobuf-topic", Proto.Person.newBuilder().setId(i).setName("Paul").build(),
						ProtobufSchema.of(Proto.Person.class));
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class SchemaTestConfig {

			@ReactivePulsarListener(id = "jsonListener", topics = "json-topic", schemaType = SchemaType.JSON,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenJson(UserPojo ignored) {
				jsonLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "avroListener", topics = "avro-topic", schemaType = SchemaType.AVRO,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenAvro(UserPojo ignored) {
				avroLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "keyvalueListener", topics = "keyvalue-topic",
					schemaType = SchemaType.KEY_VALUE, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenKeyvalue(KeyValue<String, Integer> ignored) {
				keyvalueLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "protobufListener", topics = "protobuf-topic",
					schemaType = SchemaType.PROTOBUF, consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenProtobuf(Proto.Person ignored) {
				protobufLatch.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SchemaCustomMappingsTestCases.SchemaCustomMappingsTestConfig.class)
	class SchemaCustomMappingsTestCases {

		static CountDownLatch jsonLatch = new CountDownLatch(3);
		static CountDownLatch avroLatch = new CountDownLatch(3);
		static CountDownLatch keyvalueLatch = new CountDownLatch(3);
		static CountDownLatch protobufLatch = new CountDownLatch(3);

		@Test
		void jsonSchema() throws Exception {
			PulsarProducerFactory<UserRecord> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<UserRecord> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("json-custom-schema-topic", new UserRecord("Jason", i), JSONSchema.of(UserRecord.class));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<UserPojo> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<UserPojo> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("avro-custom-schema-topic", new UserPojo("Avi", i), AvroSchema.of(UserPojo.class));
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, UserRecord>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient);
			PulsarTemplate<KeyValue<String, UserRecord>> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<KeyValue<String, UserRecord>> kvSchema = Schema.KeyValue(Schema.STRING,
					Schema.JSON(UserRecord.class), KeyValueEncodingType.INLINE);
			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-custom-schema-topic", new KeyValue<>("Kevin", new UserRecord("Kevin", 5150)),
						kvSchema);
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient);
			PulsarTemplate<Proto.Person> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("protobuf-custom-schema-topic",
						Proto.Person.newBuilder().setId(i).setName("Paul").build(),
						ProtobufSchema.of(Proto.Person.class));
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class SchemaCustomMappingsTestConfig {

			@Bean
			SchemaResolver customSchemaResolver() {
				DefaultSchemaResolver resolver = new DefaultSchemaResolver();
				resolver.addCustomSchemaMapping(UserPojo.class, Schema.AVRO(UserPojo.class));
				resolver.addCustomSchemaMapping(UserRecord.class, Schema.JSON(UserRecord.class));
				resolver.addCustomSchemaMapping(Proto.Person.class, Schema.PROTOBUF(Proto.Person.class));
				return resolver;
			}

			@Bean
			ReactivePulsarListenerContainerFactory<String> reactivePulsarListenerContainerFactory(
					ReactivePulsarConsumerFactory<String> pulsarConsumerFactory, SchemaResolver schemaResolver) {
				ReactivePulsarContainerProperties<String> containerProps = new ReactivePulsarContainerProperties<>();
				containerProps.setSchemaResolver(schemaResolver);
				return new DefaultReactivePulsarListenerContainerFactory<>(pulsarConsumerFactory, containerProps);
			}

			@ReactivePulsarListener(id = "jsonListener", topics = "json-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenJson(UserRecord ignored) {
				jsonLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "avroListener", topics = "avro-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenAvro(UserPojo ignored) {
				avroLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "keyvalueListener", topics = "keyvalue-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenKeyvalue(KeyValue<String, UserRecord> ignored) {
				keyvalueLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "protobufListener", topics = "protobuf-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenProtobuf(Proto.Person ignored) {
				protobufLatch.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = TopicCustomMappingsTestCases.TopicCustomMappingsTestConfig.class)
	class TopicCustomMappingsTestCases {

		static CountDownLatch userLatch = new CountDownLatch(3);
		static CountDownLatch stringLatch = new CountDownLatch(3);

		@Test
		void complexMessageTypeTopicMapping() throws Exception {
			PulsarProducerFactory<UserRecord> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<UserRecord> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<UserRecord> schema = Schema.JSON(UserRecord.class);
			for (int i = 0; i < 3; i++) {
				template.send("rplt-topicMapping-user-topic", new UserRecord("Jason", i), schema);
			}
			assertThat(userLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void primitiveMessageTypeTopicMapping() throws Exception {
			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient);
			PulsarTemplate<String> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("rplt-topicMapping-string-topic", "Susan " + i, Schema.STRING);
			}
			assertThat(stringLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class TopicCustomMappingsTestConfig {

			@Bean
			TopicResolver topicResolver() {
				DefaultTopicResolver resolver = new DefaultTopicResolver();
				resolver.addCustomTopicMapping(UserRecord.class, "rplt-topicMapping-user-topic");
				resolver.addCustomTopicMapping(String.class, "rplt-topicMapping-string-topic");
				return resolver;
			}

			@Bean
			ReactivePulsarListenerContainerFactory<String> reactivePulsarListenerContainerFactory(
					ReactivePulsarConsumerFactory<String> pulsarConsumerFactory, TopicResolver topicResolver) {
				ReactivePulsarContainerProperties<String> containerProps = new ReactivePulsarContainerProperties<>();
				containerProps.setTopicResolver(topicResolver);
				return new DefaultReactivePulsarListenerContainerFactory<>(pulsarConsumerFactory, containerProps);
			}

			@ReactivePulsarListener(id = "userListener", schemaType = SchemaType.JSON,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenUser(UserRecord ignored) {
				userLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "stringListener", consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenString(String ignored) {
				stringLatch.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerWithHeadersConfig.class)
	class PulsarHeadersTest {

		static CountDownLatch simpleListenerLatch = new CountDownLatch(1);
		static CountDownLatch simpleListenerPojoLatch = new CountDownLatch(1);
		static CountDownLatch pulsarMessageListenerLatch = new CountDownLatch(1);
		static CountDownLatch springMessagingMessageListenerLatch = new CountDownLatch(1);

		static AtomicReference<String> capturedData = new AtomicReference<>();
		static AtomicReference<MessageId> messageId = new AtomicReference<>();
		static AtomicReference<String> topicName = new AtomicReference<>();
		static AtomicReference<String> fooValue = new AtomicReference<>();
		static AtomicReference<Object> pojoValue = new AtomicReference<>();
		static AtomicReference<byte[]> rawData = new AtomicReference<>();

		@Test
		void simpleListenerWithHeaders() throws Exception {
			var topic = "rplt-simpleListenerWithHeaders";
			var msg = "hello-%s".formatted(topic);
			MessageId messageId = pulsarTemplate.newMessage(msg)
				.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo", "simpleListenerWithHeaders"))
				.withTopic(topic)
				.send();
			assertThat(simpleListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(PulsarHeadersTest.messageId).hasValue(messageId);
			assertThat(topicName).hasValue("persistent://public/default/%s".formatted(topic));
			assertThat(capturedData).hasValue(msg);
			assertThat(rawData).hasValue(msg.getBytes(StandardCharsets.UTF_8));
			assertThat(fooValue).hasValue("simpleListenerWithHeaders");
		}

		@Test
		void simpleListenerWithPojoHeader() throws Exception {
			var topic = "rplt-simpleListenerWithPojoHeader";
			var msg = "hello-%s".formatted(topic);
			// In order to send complex headers (pojo) must manually map and set each
			// header as follows
			var user = new UserRecord("that", 100);
			var headers = new HashMap<String, Object>();
			headers.put("user", user);
			var headerMapper = JsonPulsarHeaderMapper.builder().build();
			var mappedHeaders = headerMapper.toPulsarHeaders(new MessageHeaders(headers));
			MessageId messageId = pulsarTemplate.newMessage(msg)
				.withMessageCustomizer(messageBuilder -> mappedHeaders.forEach(messageBuilder::property))
				.withTopic(topic)
				.send();
			assertThat(simpleListenerPojoLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(PulsarHeadersTest.messageId).hasValue(messageId);
			assertThat(topicName).hasValue("persistent://public/default/%s".formatted(topic));
			assertThat(pojoValue).hasValue(user);
			assertThat(capturedData).hasValue(msg);
			assertThat(rawData).hasValue(msg.getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void pulsarMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "pulsarMessageListenerWithHeaders"))
				.withTopic("rplt-pulsarMessageListenerWithHeaders")
				.send();
			assertThat(pulsarMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData.get()).isEqualTo("hello-pulsar-message-listener");
			assertThat(PulsarHeadersTest.messageId.get()).isEqualTo(messageId);
			assertThat(topicName.get()).isEqualTo("persistent://public/default/rplt-pulsarMessageListenerWithHeaders");
			assertThat(fooValue.get()).isEqualTo("pulsarMessageListenerWithHeaders");
			assertThat(rawData.get()).isEqualTo("hello-pulsar-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void springMessagingMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "springMessagingMessageListenerWithHeaders"))
				.withTopic("rplt-springMessagingMessageListenerWithHeaders")
				.send();
			assertThat(springMessagingMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData.get()).isEqualTo("hello-spring-messaging-message-listener");
			assertThat(PulsarHeadersTest.messageId.get()).isEqualTo(messageId);
			assertThat(topicName.get())
				.isEqualTo("persistent://public/default/rplt-springMessagingMessageListenerWithHeaders");
			assertThat(fooValue.get()).isEqualTo("springMessagingMessageListenerWithHeaders");
			assertThat(rawData.get())
				.isEqualTo("hello-spring-messaging-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@EnableReactivePulsar
		@Configuration
		static class PulsarListenerWithHeadersConfig {

			@ReactivePulsarListener(topics = "rplt-simpleListenerWithHeaders",
					subscriptionName = "rplt-simple-listener-with-headers-sub",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> simpleListenerWithHeaders(String data, @Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("foo") String foo) {
				capturedData.set(data);
				PulsarHeadersTest.messageId.set(messageId);
				PulsarHeadersTest.topicName.set(topicName);
				fooValue.set(foo);
				PulsarHeadersTest.rawData.set(rawData);
				simpleListenerLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(topics = "rplt-simpleListenerWithPojoHeader",
					subscriptionName = "simpleListenerWithPojoHeader-sub",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> simpleListenerWithPojoHeader(String data, @Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("user") UserRecord user) {
				capturedData.set(data);
				PulsarHeadersTest.messageId.set(messageId);
				PulsarHeadersTest.topicName.set(topicName);
				pojoValue.set(user);
				PulsarHeadersTest.rawData.set(rawData);
				simpleListenerPojoLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(subscriptionName = "rplt-pulsar-message-listener-with-headers-sub",
					topics = "rplt-pulsarMessageListenerWithHeaders",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> pulsarMessageListenerWithHeaders(Message<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("foo") String foo) {
				capturedData.set(data.getValue());
				PulsarHeadersTest.messageId.set(messageId);
				PulsarHeadersTest.topicName.set(topicName);
				fooValue.set(foo);
				PulsarHeadersTest.rawData.set(rawData);
				pulsarMessageListenerLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(subscriptionName = "rplt-pulsar-message-listener-with-headers-sub",
					topics = "rplt-springMessagingMessageListenerWithHeaders",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> springMessagingMessageListenerWithHeaders(org.springframework.messaging.Message<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.RAW_DATA) byte[] rawData, @Header(PulsarHeaders.TOPIC_NAME) String topicName,
					@Header("foo") String foo) {
				capturedData.set(data.getPayload());
				PulsarHeadersTest.messageId.set(messageId);
				PulsarHeadersTest.topicName.set(topicName);
				fooValue.set(foo);
				PulsarHeadersTest.rawData.set(rawData);
				springMessagingMessageListenerLatch.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarHeadersCustomObjectMapperTestConfig.class)
	class PulsarHeadersCustomObjectMapperTest {

		private static final String TOPIC = "rplt-listenerWithPojoHeader-custom";

		private static final CountDownLatch listenerLatch = new CountDownLatch(1);

		private static UserRecord userPassedIntoListener;

		@Test
		void whenPulsarHeaderObjectMapperIsDefinedThenItIsUsedToDeserializeHeaders() throws Exception {
			var msg = "hello-%s".formatted(TOPIC);
			// In order to send complex headers (pojo) must manually map and set each
			// header as follows
			var user = new UserRecord("that", 100);
			var headers = new HashMap<String, Object>();
			headers.put("user", user);
			var headerMapper = JsonPulsarHeaderMapper.builder().build();
			var mappedHeaders = headerMapper.toPulsarHeaders(new MessageHeaders(headers));
			pulsarTemplate.newMessage(msg)
				.withMessageCustomizer(messageBuilder -> mappedHeaders.forEach(messageBuilder::property))
				.withTopic(TOPIC)
				.send();
			// Custom deser adds suffix to name and bumps age + 5
			var expectedUser = new UserRecord(user.name() + "-deser", user.age() + 5);
			assertThat(listenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(userPassedIntoListener).isEqualTo(expectedUser);
		}

		@Configuration(proxyBeanMethods = false)
		static class PulsarHeadersCustomObjectMapperTestConfig {

			@Bean(name = "pulsarHeaderObjectMapper")
			ObjectMapper customObjectMapper() {
				var objectMapper = new ObjectMapper();
				var module = new SimpleModule();
				module.addDeserializer(UserRecord.class, new UserRecordDeserializer());
				objectMapper.registerModule(module);
				return objectMapper;
			}

			@ReactivePulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenerWithPojoHeader(String data, @Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("user") UserRecord user) {
				userPassedIntoListener = user;
				listenerLatch.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerConcurrencyTestCases.TestPulsarListenersForConcurrency.class)
	class PulsarListenerConcurrencyTestCases {

		static CountDownLatch latch = new CountDownLatch(100);

		static BlockingQueue<String> queue = new LinkedBlockingQueue<>();

		@Test
		void pulsarListenerWithConcurrency() throws Exception {
			for (int i = 0; i < 100; i++) {
				pulsarTemplate.send("pulsarListenerConcurrency", "hello foo");
			}
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void pulsarListenerWithConcurrencyKeyOrdered() throws Exception {
			pulsarTemplate.newMessage("first")
				.withTopic("pulsarListenerWithConcurrencyKeyOrdered")
				.withMessageCustomizer(m -> m.key("key"))
				.send();
			pulsarTemplate.newMessage("second")
				.withTopic("pulsarListenerWithConcurrencyKeyOrdered")
				.withMessageCustomizer(m -> m.key("key"))
				.send();
			assertThat(queue.poll(5, TimeUnit.SECONDS)).isEqualTo("first");
			assertThat(queue.poll(5, TimeUnit.SECONDS)).isEqualTo("second");
		}

		@EnableReactivePulsar
		@Configuration
		static class TestPulsarListenersForConcurrency {

			@ReactivePulsarListener(topics = "pulsarListenerConcurrency",
					consumerCustomizer = "subscriptionInitialPositionEarliest", concurrency = "100")
			Mono<Void> listen1(String ignored) {
				latch.countDown();
				// if messages are not handled concurrently, this will make the latch
				// await timeout.
				return Mono.delay(Duration.ofMillis(100)).then();
			}

			@ReactivePulsarListener(topics = "pulsarListenerWithConcurrencyKeyOrdered",
					consumerCustomizer = "subscriptionInitialPositionEarliest", concurrency = "100",
					useKeyOrderedProcessing = "true")
			Mono<Void> listen2(String message) {
				if (message.equals("first")) {
					// if message processing is not ordered by keys, "first" will be added
					// to the queue after "second"
					return Mono.delay(Duration.ofMillis(1000)).doOnNext(m -> queue.add(message)).then();
				}
				queue.add(message);
				return Mono.empty();
			}

		}

	}

	@Nested
	class SubscriptionTypeTests {

		@Nested
		@ContextConfiguration(classes = WithDefaultTypeConfig.class)
		class WithDefaultType {

			static final CountDownLatch latchTypeNotSet = new CountDownLatch(1);

			@Test
			void whenTypeNotSetAnywhereThenFallbackTypeIsUsed(
					@Autowired ConsumerTrackingReactivePulsarConsumerFactory<String> consumerFactory) throws Exception {
				assertThat(consumerFactory.topicNameToConsumerSpec).hasEntrySatisfying("rpl-typeNotSetAnywhere-topic",
						(consumerSpec) -> assertThat(consumerSpec.getSubscriptionType())
							.isEqualTo(SubscriptionType.Exclusive));
				pulsarTemplate.send("rpl-typeNotSetAnywhere-topic", "hello-rpl-typeNotSetAnywhere");
				assertThat(latchTypeNotSet.await(10, TimeUnit.SECONDS)).isTrue();
			}

			@Configuration(proxyBeanMethods = false)
			static class WithDefaultTypeConfig {

				@ReactivePulsarListener(topics = "rpl-typeNotSetAnywhere-topic",
						subscriptionName = "rpl-typeNotSetAnywhere-sub",
						consumerCustomizer = "subscriptionInitialPositionEarliest")
				Mono<Void> listenWithoutTypeSetAnywhere(String ignored) {
					latchTypeNotSet.countDown();
					return Mono.empty();
				}

			}

		}

		@Nested
		@ContextConfiguration(classes = WithSpecificTypesConfig.class)
		class WithSpecificTypes {

			static final CountDownLatch latchTypeSetConsumerFactory = new CountDownLatch(1);

			static final CountDownLatch latchTypeSetAnnotation = new CountDownLatch(1);

			static final CountDownLatch latchWithCustomizer = new CountDownLatch(1);

			@Test
			void whenTypeSetOnlyInConsumerFactoryThenConsumerFactoryTypeIsUsed(
					@Autowired ConsumerTrackingReactivePulsarConsumerFactory<String> consumerFactory) throws Exception {
				assertThat(consumerFactory.getSpec("rpl-typeSetConsumerFactory-topic"))
					.extracting(ReactiveMessageConsumerSpec::getSubscriptionType)
					.isEqualTo(SubscriptionType.Shared);
				pulsarTemplate.send("rpl-typeSetConsumerFactory-topic", "hello-rpl-typeSetConsumerFactory");
				assertThat(latchTypeSetConsumerFactory.await(10, TimeUnit.SECONDS)).isTrue();
			}

			@Test
			void whenTypeSetOnAnnotationThenAnnotationTypeIsUsed(
					@Autowired ConsumerTrackingReactivePulsarConsumerFactory<String> consumerFactory) throws Exception {
				assertThat(consumerFactory.getSpec("rpl-typeSetAnnotation-topic"))
					.extracting(ReactiveMessageConsumerSpec::getSubscriptionType)
					.isEqualTo(SubscriptionType.Key_Shared);
				pulsarTemplate.send("rpl-typeSetAnnotation-topic", "hello-rpl-typeSetAnnotation");
				assertThat(latchTypeSetAnnotation.await(10, TimeUnit.SECONDS)).isTrue();
			}

			@Test
			void whenTypeSetWithCustomizerThenCustomizerTypeIsUsed(
					@Autowired ConsumerTrackingReactivePulsarConsumerFactory<String> consumerFactory) throws Exception {
				assertThat(consumerFactory.getSpec("rpl-typeSetCustomizer-topic"))
					.extracting(ReactiveMessageConsumerSpec::getSubscriptionType)
					.isEqualTo(SubscriptionType.Failover);
				pulsarTemplate.send("rpl-typeSetCustomizer-topic", "hello-rpl-typeSetCustomizer");
				assertThat(latchWithCustomizer.await(10, TimeUnit.SECONDS)).isTrue();
			}

			@Configuration(proxyBeanMethods = false)
			static class WithSpecificTypesConfig {

				@Bean
				ReactiveMessageConsumerBuilderCustomizer<String> consumerFactoryDefaultSubTypeCustomizer() {
					return (b) -> b.subscriptionType(SubscriptionType.Shared);
				}

				@ReactivePulsarListener(topics = "rpl-typeSetConsumerFactory-topic",
						subscriptionName = "rpl-typeSetConsumerFactory-sub", subscriptionType = {},
						consumerCustomizer = "subscriptionInitialPositionEarliest")
				Mono<Void> listenWithTypeSetOnlyOnConsumerFactory(String ignored) {
					latchTypeSetConsumerFactory.countDown();
					return Mono.empty();
				}

				@ReactivePulsarListener(topics = "rpl-typeSetAnnotation-topic",
						subscriptionName = "rpl-typeSetAnnotation-sub", subscriptionType = SubscriptionType.Key_Shared,
						consumerCustomizer = "subscriptionInitialPositionEarliest")
				Mono<Void> listenWithTypeSetOnAnnotation(String ignored) {
					latchTypeSetAnnotation.countDown();
					return Mono.empty();
				}

				@ReactivePulsarListener(topics = "rpl-typeSetCustomizer-topic",
						subscriptionName = "rpl-typeSetCustomizer-sub", subscriptionType = SubscriptionType.Key_Shared,
						consumerCustomizer = "myCustomizer")
				Mono<Void> listenWithTypeSetInCustomizer(String ignored) {
					latchWithCustomizer.countDown();
					return Mono.empty();
				}

				@Bean
				public ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> myCustomizer() {
					return cb -> cb.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
						.subscriptionType(SubscriptionType.Failover);
				}

			}

		}

	}

	static class ConsumerTrackingReactivePulsarConsumerFactory<T> implements ReactivePulsarConsumerFactory<T> {

		private Map<String, ReactiveMessageConsumerSpec> topicNameToConsumerSpec = new HashMap<>();

		private ReactivePulsarConsumerFactory<T> delegate;

		ConsumerTrackingReactivePulsarConsumerFactory(ReactivePulsarConsumerFactory<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema) {
			var consumer = this.delegate.createConsumer(schema);
			storeSpec(consumer);
			return consumer;
		}

		@Override
		public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema,
				List<ReactiveMessageConsumerBuilderCustomizer<T>> reactiveMessageConsumerBuilderCustomizers) {
			var consumer = this.delegate.createConsumer(schema, reactiveMessageConsumerBuilderCustomizers);
			storeSpec(consumer);
			return consumer;
		}

		private void storeSpec(ReactiveMessageConsumer<T> consumer) {
			var consumerSpec = (ReactiveMessageConsumerSpec) ReflectionTestUtils.getField(consumer, "consumerSpec");
			var topicNamesKey = !ObjectUtils.isEmpty(consumerSpec.getTopicNames()) ? consumerSpec.getTopicNames().get(0)
					: "no-topics-set";
			this.topicNameToConsumerSpec.put(topicNamesKey, consumerSpec);
		}

		ReactiveMessageConsumerSpec getSpec(String topic) {
			return this.topicNameToConsumerSpec.get(topic);
		}

	}

}

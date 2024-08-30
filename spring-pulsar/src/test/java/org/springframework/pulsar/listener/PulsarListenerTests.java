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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.pulsar.listener.PulsarListenerTests.PulsarHeadersTest.PulsarListenerWithHeadersConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.annotation.PulsarListenerConsumerBuilderCustomizer;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.listener.PulsarListenerTests.PulsarHeadersCustomObjectMapperTest.PulsarHeadersCustomObjectMapperTestConfig;
import org.springframework.pulsar.listener.PulsarListenerTests.SubscriptionTypeTests.SubscriptionTypeTestsConfig;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper;
import org.springframework.pulsar.test.model.UserPojo;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordDeserializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 */
class PulsarListenerTests extends PulsarListenerTestsBase {

	@Nested
	@ContextConfiguration(classes = PulsarListenerBasicTestCases.TestPulsarListenersForBasicScenario.class)
	class PulsarListenerBasicTestCases {

		static CountDownLatch latch = new CountDownLatch(1);
		static CountDownLatch latch1 = new CountDownLatch(3);
		static CountDownLatch latch2 = new CountDownLatch(3);

		@Test
		void pulsarListenerWithTopicsPattern(@Autowired PulsarListenerEndpointRegistry registry) throws Exception {
			PulsarMessageListenerContainer baz = registry.getListenerContainer("baz");
			PulsarContainerProperties containerProperties = baz.getContainerProperties();
			assertThat(containerProperties.getTopicsPattern()).isEqualTo("persistent://public/default/pattern.*");

			pulsarTemplate.send("persistent://public/default/pattern-1", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-2", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-3", "hello baz");

			assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void pulsarListenerProvidedConsumerProperties(@Autowired PulsarListenerEndpointRegistry registry)
				throws Exception {

			PulsarContainerProperties pulsarContainerProperties = registry.getListenerContainer("foo")
				.getContainerProperties();
			Properties pulsarConsumerProperties = pulsarContainerProperties.getPulsarConsumerProperties();
			assertThat(pulsarConsumerProperties.size()).isEqualTo(2);
			assertThat(pulsarConsumerProperties.get("topicNames")).isEqualTo("foo-1");
			assertThat(pulsarConsumerProperties.get("subscriptionName")).isEqualTo("subscription-1");
			pulsarTemplate.send("hello foo");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void concurrencyOnPulsarListenerWithFailoverSubscription(@Autowired PulsarListenerEndpointRegistry registry)
				throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<String>(pulsarClient, null,
					List.of((pb) -> pb.enableBatching(false)));
			var customTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			var bar = (ConcurrentPulsarMessageListenerContainer<?>) registry.getListenerContainer("bar");

			assertThat(bar.getConcurrency()).isEqualTo(3);

			customTemplate.sendAsync("concurrency-on-pl", "hello john doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello alice doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello buzz doe");

			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void nonDefaultConcurrencySettingNotAllowedOnExclusiveSubscriptions(
				@Autowired PulsarListenerEndpointRegistry registry) throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<String>(pulsarClient, null,
					List.of((pb) -> pb.enableBatching(false)));
			var customTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			var bar = (ConcurrentPulsarMessageListenerContainer<?>) registry.getListenerContainer("bar");

			assertThat(bar.getConcurrency()).isEqualTo(3);

			customTemplate.sendAsync("concurrency-on-pl", "hello john doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello alice doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello buzz doe");

			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void ackModeAppliedToContainerFromListener(@Autowired PulsarListenerEndpointRegistry registry) {
			PulsarContainerProperties pulsarContainerProperties = registry.getListenerContainer("ackMode-test-id")
				.getContainerProperties();
			assertThat(pulsarContainerProperties.getAckMode()).isEqualTo(AckMode.RECORD);
		}

		@EnablePulsar
		@Configuration
		static class TestPulsarListenersForBasicScenario {

			@PulsarListener(id = "foo", properties = { "subscriptionName=subscription-1", "topicNames=foo-1" })
			void listen1(String ignored) {
				latch.countDown();
			}

			@PulsarListener(id = "bar", topics = "concurrency-on-pl", subscriptionName = "subscription-2",
					subscriptionType = SubscriptionType.Failover, concurrency = "3")
			void listen2(String ignored) {
				latch1.countDown();
			}

			@PulsarListener(id = "baz", topicPattern = "persistent://public/default/pattern.*",
					subscriptionName = "subscription-3",
					properties = { "patternAutoDiscoveryPeriod=5", "subscriptionInitialPosition=Earliest" })
			void listen3(String ignored) {
				latch2.countDown();
			}

			@PulsarListener(id = "ackMode-test-id", subscriptionName = "ackModeTest-sub", topics = "ackModeTest-topic",
					ackMode = AckMode.RECORD)
			void ackModeTestListener(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = NegativeAckRedeliveryBackoffTest.NegativeAckRedeliveryConfig.class)
	class NegativeAckRedeliveryBackoffTest {

		static CountDownLatch nackRedeliveryBackoffLatch = new CountDownLatch(5);

		@Test
		void pulsarListenerWithNackRedeliveryBackoff() throws Exception {
			pulsarTemplate.send("withNegRedeliveryBackoff-test-topic", "hello john doe");
			assertThat(nackRedeliveryBackoffLatch.await(15, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class NegativeAckRedeliveryConfig {

			@PulsarListener(id = "withNegRedeliveryBackoff", subscriptionName = "withNegRedeliveryBackoffSubscription",
					topics = "withNegRedeliveryBackoff-test-topic", negativeAckRedeliveryBackoff = "redeliveryBackoff",
					subscriptionType = SubscriptionType.Shared)
			void listen(String msg) {
				nackRedeliveryBackoffLatch.countDown();
				throw new RuntimeException("fail " + msg);
			}

			@Bean
			public RedeliveryBackoff redeliveryBackoff() {
				return MultiplierRedeliveryBackoff.builder()
					.minDelayMs(1000)
					.maxDelayMs(5 * 1000)
					.multiplier(2)
					.build();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = AckTimeoutkRedeliveryBackoffTest.AckTimeoutRedeliveryConfig.class)
	class AckTimeoutkRedeliveryBackoffTest {

		static CountDownLatch ackTimeoutRedeliveryBackoffLatch = new CountDownLatch(3);

		@Test
		void pulsarListenerWithAckTimeoutRedeliveryBackoff() throws Exception {
			pulsarTemplate.send("withAckTimeoutRedeliveryBackoff-test-topic", "hello john doe");
			assertThat(ackTimeoutRedeliveryBackoffLatch.await(60, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class AckTimeoutRedeliveryConfig {

			/**
			 * The following PulsarListener is for testing the ack timeout settings. We
			 * set an ack timeout of 1 second in the listener and then will not ack the
			 * incoming message unless the tracking CountDownLatch count goes down to
			 * zero. This means that there were enough ack timeouts and corresponding
			 * redeliveries of the same message. Note that we are doing a manual ack
			 * because we want to ack the message only when the latch count becomes zero.
			 */
			@PulsarListener(id = "withAckTimeoutRedeliveryBackoff",
					subscriptionName = "withAckTimeoutRedeliveryBackoffSubscription",
					topics = "withAckTimeoutRedeliveryBackoff-test-topic",
					ackTimeoutRedeliveryBackoff = "ackTimeoutRedeliveryBackoff", ackMode = AckMode.MANUAL,
					subscriptionType = SubscriptionType.Shared, properties = { "ackTimeoutMillis=1000" })
			void listen(String ignored, Acknowledgement acknowledgement) {
				ackTimeoutRedeliveryBackoffLatch.countDown();
				if (ackTimeoutRedeliveryBackoffLatch.getCount() == 0) {
					acknowledgement.acknowledge();
				}
			}

			@Bean
			public RedeliveryBackoff ackTimeoutRedeliveryBackoff() {
				return MultiplierRedeliveryBackoff.builder()
					.minDelayMs(1000)
					.maxDelayMs(3 * 1000)
					.multiplier(2)
					.build();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarConsumerErrorHandlerTest.PulsarConsumerErrorHandlerConfig.class)
	class PulsarConsumerErrorHandlerTest {

		private static CountDownLatch pulsarConsumerErrorHandlerLatch = new CountDownLatch(11);

		private static CountDownLatch dltLatch = new CountDownLatch(1);

		@Test
		void pulsarListenerWithNackRedeliveryBackoff(@Autowired PulsarListenerEndpointRegistry registry)
				throws Exception {
			pulsarTemplate.send("pceht-topic", "hello john doe");
			assertThat(pulsarConsumerErrorHandlerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(dltLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class PulsarConsumerErrorHandlerConfig {

			@PulsarListener(id = "pceht-id", subscriptionName = "pceht-subscription", topics = "pceht-topic",
					pulsarConsumerErrorHandler = "pulsarConsumerErrorHandler")
			void listen(String msg) {
				pulsarConsumerErrorHandlerLatch.countDown();
				throw new RuntimeException("fail " + msg);
			}

			@PulsarListener(id = "pceh-dltListener", topics = "pceht-topic-pceht-subscription-DLT")
			void listenDlq(String msg) {
				dltLatch.countDown();
			}

			@Bean
			public PulsarConsumerErrorHandler<String> pulsarConsumerErrorHandler(
					PulsarTemplate<String> pulsarTemplate) {
				return new DefaultPulsarConsumerErrorHandler<>(
						new PulsarDeadLetterPublishingRecoverer<>(pulsarTemplate), new FixedBackOff(100, 10));
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

		@EnablePulsar
		@Configuration
		static class DeadLetterPolicyConfig {

			@PulsarListener(id = "deadLetterPolicyListener", subscriptionName = "deadLetterPolicySubscription",
					topics = "dlpt-topic-1", deadLetterPolicy = "deadLetterPolicy",
					subscriptionType = SubscriptionType.Shared,
					properties = { "negativeAckRedeliveryDelayMicros=1000000" })
			void listen(String msg) {
				latch.countDown();
				throw new RuntimeException("fail " + msg);
			}

			@PulsarListener(id = "dlqListener", topics = "dlpt-dlq-topic")
			void listenDlq(String ignored) {
				dlqLatch.countDown();
			}

			@Bean
			DeadLetterPolicy deadLetterPolicy() {
				return DeadLetterPolicy.builder().maxRedeliverCount(1).deadLetterTopic("dlpt-dlq-topic").build();
			}

		}

	}

	@Nested
	class NegativeConcurrency {

		@Test
		void exclusiveSubscriptionNotAllowedToHaveMultipleConsumers() {
			assertThatThrownBy(
					() -> new AnnotationConfigApplicationContext(TopLevelConfig.class, ConcurrencyConfig.class))
				.rootCause()
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("concurrency > 1 is not allowed on Exclusive subscription type");
		}

		@EnablePulsar
		static class ConcurrencyConfig {

			@PulsarListener(id = "foobar", topics = "concurrency-on-pl", subscriptionName = "subscription-3",
					concurrency = "3")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SchemaTestCases.SchemaTestConfig.class)
	class SchemaTestCases {

		static CountDownLatch jsonLatch = new CountDownLatch(3);
		static CountDownLatch jsonBatchLatch = new CountDownLatch(3);
		static CountDownLatch avroLatch = new CountDownLatch(3);
		static CountDownLatch avroBatchLatch = new CountDownLatch(3);
		static CountDownLatch keyvalueLatch = new CountDownLatch(3);
		static CountDownLatch keyvalueBatchLatch = new CountDownLatch(3);
		static CountDownLatch protobufLatch = new CountDownLatch(3);
		static CountDownLatch protobufBatchLatch = new CountDownLatch(3);

		@Test
		void jsonSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserPojo>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = JSONSchema.of(UserPojo.class);
			for (int i = 0; i < 3; i++) {
				template.send("json-topic", new UserPojo("Jason", i), schema);
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(jsonBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserPojo>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = AvroSchema.of(UserPojo.class);
			for (int i = 0; i < 3; i++) {
				template.send("avro-topic", new UserPojo("Avi", i), schema);
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(avroBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<KeyValue<String, Integer>>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var kvSchema = Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE);
			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-topic", new KeyValue<>("Kevin", i), kvSchema);
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(keyvalueBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Proto.Person>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = ProtobufSchema.of(Proto.Person.class);
			for (int i = 0; i < 3; i++) {
				template.send("protobuf-topic", Proto.Person.newBuilder().setId(i).setName("Paul").build(), schema);
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(protobufBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class SchemaTestConfig {

			@PulsarListener(id = "jsonListener", topics = "json-topic", subscriptionName = "subscription-4",
					schemaType = SchemaType.JSON, properties = { "subscriptionInitialPosition=Earliest" })
			void listenJson(UserPojo ignored) {
				jsonLatch.countDown();
			}

			@PulsarListener(id = "jsonBatchListener", topics = "json-topic", subscriptionName = "subscription-5",
					schemaType = SchemaType.JSON, batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			void listenJsonBatch(List<UserPojo> messages) {
				messages.forEach(m -> jsonBatchLatch.countDown());
			}

			@PulsarListener(id = "avroListener", topics = "avro-topic", subscriptionName = "subscription-6",
					schemaType = SchemaType.AVRO, properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvro(UserPojo ignored) {
				avroLatch.countDown();
			}

			@PulsarListener(id = "avroBatchListener", topics = "avro-topic", subscriptionName = "subscription-7",
					schemaType = SchemaType.AVRO, batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvroBatch(Messages<UserPojo> messages) {
				messages.forEach(m -> avroBatchLatch.countDown());
			}

			@PulsarListener(id = "keyvalueListener", topics = "keyvalue-topic", subscriptionName = "subscription-8",
					schemaType = SchemaType.KEY_VALUE, properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalue(KeyValue<String, Integer> ignored) {
				keyvalueLatch.countDown();
			}

			@PulsarListener(id = "keyvalueBatchListener", topics = "keyvalue-topic",
					subscriptionName = "subscription-9", schemaType = SchemaType.KEY_VALUE, batch = true,
					properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalueBatch(List<KeyValue<String, Integer>> messages) {
				messages.forEach(m -> keyvalueBatchLatch.countDown());
			}

			@PulsarListener(id = "protobufListener", topics = "protobuf-topic", subscriptionName = "subscription-10",
					schemaType = SchemaType.PROTOBUF, properties = { "subscriptionInitialPosition=Earliest" })
			void listenProtobuf(Proto.Person ignored) {
				protobufLatch.countDown();
			}

			@PulsarListener(id = "protobufBatchListener", topics = "protobuf-topic",
					subscriptionName = "subscription-11", schemaType = SchemaType.PROTOBUF, batch = true,
					properties = { "subscriptionInitialPosition=Earliest" })
			void listenProtobufBatch(List<Proto.Person> messages) {
				messages.forEach(m -> protobufBatchLatch.countDown());
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
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserRecord>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = Schema.JSON(UserRecord.class);
			for (int i = 0; i < 3; i++) {
				template.send("json-custom-mappings-topic", new UserRecord("Jason", i), schema);
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserPojo>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = AvroSchema.of(UserPojo.class);
			for (int i = 0; i < 3; i++) {
				template.send("avro-custom-mappings-topic", new UserPojo("Avi", i), schema);
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<KeyValue<String, UserRecord>>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var kvSchema = Schema.KeyValue(Schema.STRING, Schema.JSON(UserRecord.class), KeyValueEncodingType.INLINE);
			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-custom-mappings-topic", new KeyValue<>("Kevin", new UserRecord("Kevin", 5150)),
						kvSchema);
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<Proto.Person>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = ProtobufSchema.of(Proto.Person.class);
			for (int i = 0; i < 3; i++) {
				template.send("protobuf-custom-mappings-topic",
						Proto.Person.newBuilder().setId(i).setName("Paul").build(), schema);
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
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
			PulsarListenerContainerFactory pulsarListenerContainerFactory(
					PulsarConsumerFactory<Object> pulsarConsumerFactory, SchemaResolver schemaResolver) {
				PulsarContainerProperties containerProps = new PulsarContainerProperties();
				containerProps.setSchemaResolver(schemaResolver);
				ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>(
						pulsarConsumerFactory, containerProps);
				return pulsarListenerContainerFactory;
			}

			@PulsarListener(id = "jsonListener", topics = "json-custom-mappings-topic",
					subscriptionName = "subscription-4", properties = { "subscriptionInitialPosition=Earliest" })
			void listenJson(UserRecord ignored) {
				jsonLatch.countDown();
			}

			@PulsarListener(id = "avroListener", topics = "avro-custom-mappings-topic",
					subscriptionName = "subscription-6", properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvro(UserPojo ignored) {
				avroLatch.countDown();
			}

			@PulsarListener(id = "keyvalueListener", topics = "keyvalue-custom-mappings-topic",
					subscriptionName = "subscription-8", properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalue(KeyValue<String, UserRecord> ignored) {
				keyvalueLatch.countDown();
			}

			@PulsarListener(id = "protobufListener", topics = "protobuf-custom-mappings-topic",
					subscriptionName = "subscription-10", properties = { "subscriptionInitialPosition=Earliest" })
			void listenProtobuf(Proto.Person ignored) {
				protobufLatch.countDown();
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
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserRecord>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			var schema = Schema.JSON(UserRecord.class);
			for (int i = 0; i < 3; i++) {
				template.send("plt-topicMapping-user-topic", new UserRecord("Jason", i), schema);
			}
			assertThat(userLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void primitiveMessageTypeTopicMapping() throws Exception {
			var pulsarProducerFactory = new DefaultPulsarProducerFactory<String>(pulsarClient);
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("plt-topicMapping-string-topic", "Susan " + i, Schema.STRING);
			}
			assertThat(stringLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class TopicCustomMappingsTestConfig {

			@Bean
			TopicResolver topicResolver() {
				DefaultTopicResolver resolver = new DefaultTopicResolver();
				resolver.addCustomTopicMapping(UserRecord.class, "plt-topicMapping-user-topic");
				resolver.addCustomTopicMapping(String.class, "plt-topicMapping-string-topic");
				return resolver;
			}

			@Bean
			PulsarListenerContainerFactory pulsarListenerContainerFactory(
					PulsarConsumerFactory<Object> pulsarConsumerFactory, TopicResolver topicResolver) {
				PulsarContainerProperties containerProps = new PulsarContainerProperties();
				containerProps.setTopicResolver(topicResolver);
				ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>(
						pulsarConsumerFactory, containerProps);
				return pulsarListenerContainerFactory;
			}

			@PulsarListener(id = "userListener", schemaType = SchemaType.JSON, subscriptionName = "sub1",
					properties = { "subscriptionInitialPosition=Earliest" })
			void listenUser(UserRecord ignored) {
				userLatch.countDown();
			}

			@PulsarListener(id = "stringListener", subscriptionName = "sub2",
					properties = { "subscriptionInitialPosition=Earliest" })
			void listenString(String ignored) {
				stringLatch.countDown();
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

		static volatile String capturedData;
		static volatile MessageId messageId;
		static volatile String topicName;
		static volatile String fooValue;
		static volatile Object pojoValue;
		static volatile byte[] rawData;

		static CountDownLatch simpleBatchListenerLatch = new CountDownLatch(1);
		static CountDownLatch pulsarMessageBatchListenerLatch = new CountDownLatch(1);
		static CountDownLatch springMessagingMessageBatchListenerLatch = new CountDownLatch(1);
		static CountDownLatch pulsarMessagesBatchListenerLatch = new CountDownLatch(1);

		static volatile List<String> capturedBatchData;
		static volatile List<MessageId> batchMessageIds;
		static volatile List<String> batchTopicNames;
		static volatile List<String> batchFooValues;

		@Test
		void simpleListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-simple-listener")
				.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo", "simpleListenerWithHeaders"))
				.withTopic("plt-simpleListenerWithHeaders")
				.send();
			assertThat(simpleListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-simple-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/plt-simpleListenerWithHeaders");
			assertThat(fooValue).isEqualTo("simpleListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-simple-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void simpleListenerWithPojoHeader() throws Exception {
			var topic = "plt-simpleListenerWithPojoHeader";
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
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/%s".formatted(topic));
			assertThat(pojoValue).isEqualTo(user);
			assertThat(capturedData).isEqualTo(msg);
			assertThat(rawData).isEqualTo(msg.getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void pulsarMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "pulsarMessageListenerWithHeaders"))
				.withTopic("plt-pulsarMessageListenerWithHeaders")
				.send();
			assertThat(pulsarMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-pulsar-message-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/plt-pulsarMessageListenerWithHeaders");
			assertThat(fooValue).isEqualTo("pulsarMessageListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-pulsar-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void springMessagingMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "springMessagingMessageListenerWithHeaders"))
				.withTopic("plt-springMessagingMessageListenerWithHeaders")
				.send();
			assertThat(springMessagingMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-spring-messaging-message-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName)
				.isEqualTo("persistent://public/default/plt-springMessagingMessageListenerWithHeaders");
			assertThat(fooValue).isEqualTo("springMessagingMessageListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-spring-messaging-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void simpleBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-simple-batch-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "simpleBatchListenerWithHeaders"))
				.withTopic("plt-simpleBatchListenerWithHeaders")
				.send();
			assertThat(simpleBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-simple-batch-listener");
			assertThat(batchMessageIds).containsExactly(messageId);
			assertThat(batchTopicNames)
				.containsExactly("persistent://public/default/plt-simpleBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("simpleBatchListenerWithHeaders");
		}

		@Test
		void pulsarMessageBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-batch-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "pulsarMessageBatchListenerWithHeaders"))
				.withTopic("plt-pulsarMessageBatchListenerWithHeaders")
				.send();
			assertThat(pulsarMessageBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-pulsar-message-batch-listener");
			assertThat(batchTopicNames)
				.containsExactly("persistent://public/default/plt-pulsarMessageBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("pulsarMessageBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@Test
		void springMessagingMessageBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-batch-listener")
				.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo",
						"springMessagingMessageBatchListenerWithHeaders"))
				.withTopic("plt-springMessagingMessageBatchListenerWithHeaders")
				.send();
			assertThat(springMessagingMessageBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-spring-messaging-message-batch-listener");
			assertThat(batchTopicNames)
				.containsExactly("persistent://public/default/plt-springMessagingMessageBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("springMessagingMessageBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@Test
		void pulsarMessagesBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-messages-batch-listener")
				.withMessageCustomizer(
						messageBuilder -> messageBuilder.property("foo", "pulsarMessagesBatchListenerWithHeaders"))
				.withTopic("plt-pulsarMessagesBatchListenerWithHeaders")
				.send();
			assertThat(pulsarMessagesBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-pulsar-messages-batch-listener");
			assertThat(batchTopicNames)
				.containsExactly("persistent://public/default/plt-pulsarMessagesBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("pulsarMessagesBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@EnablePulsar
		@Configuration
		static class PulsarListenerWithHeadersConfig {

			@PulsarListener(subscriptionName = "plt-simple-listener-with-headers-sub",
					topics = "plt-simpleListenerWithHeaders")
			void simpleListenerWithHeaders(String data, @Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("foo") String foo) {
				capturedData = data;
				PulsarHeadersTest.messageId = messageId;
				PulsarHeadersTest.topicName = topicName;
				fooValue = foo;
				PulsarHeadersTest.rawData = rawData;
				simpleListenerLatch.countDown();
			}

			@PulsarListener(topics = "plt-simpleListenerWithPojoHeader",
					subscriptionName = "plt-simpleListenerWithPojoHeader-sub")
			void simpleListenerWithPojoHeader(String data, @Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("user") UserRecord user) {
				capturedData = data;
				PulsarHeadersTest.messageId = messageId;
				PulsarHeadersTest.topicName = topicName;
				pojoValue = user;
				PulsarHeadersTest.rawData = rawData;
				simpleListenerPojoLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-pulsar-message-listener-with-headers-sub",
					topics = "plt-pulsarMessageListenerWithHeaders")
			void pulsarMessageListenerWithHeaders(Message<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.TOPIC_NAME) String topicName, @Header(PulsarHeaders.RAW_DATA) byte[] rawData,
					@Header("foo") String foo) {
				capturedData = data.getValue();
				PulsarHeadersTest.messageId = messageId;
				PulsarHeadersTest.topicName = topicName;
				fooValue = foo;
				PulsarHeadersTest.rawData = rawData;
				pulsarMessageListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-pulsar-message-listener-with-headers-sub",
					topics = "plt-springMessagingMessageListenerWithHeaders")
			void springMessagingMessageListenerWithHeaders(org.springframework.messaging.Message<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) MessageId messageId,
					@Header(PulsarHeaders.RAW_DATA) byte[] rawData, @Header(PulsarHeaders.TOPIC_NAME) String topicName,
					@Header("foo") String foo) {
				capturedData = data.getPayload();
				PulsarHeadersTest.messageId = messageId;
				PulsarHeadersTest.topicName = topicName;
				fooValue = foo;
				PulsarHeadersTest.rawData = rawData;
				springMessagingMessageListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-simple-batch-listener-with-headers-sub",
					topics = "plt-simpleBatchListenerWithHeaders", batch = true)
			void simpleBatchListenerWithHeaders(List<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {
				capturedBatchData = data;
				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				simpleBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-pulsarMessage-batch-listener-with-headers-sub",
					topics = "plt-pulsarMessageBatchListenerWithHeaders", batch = true)
			void pulsarMessageBatchListenerWithHeaders(List<Message<String>> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {
				capturedBatchData = data.stream().map(Message::getValue).collect(Collectors.toList());
				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				pulsarMessageBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-spring-messaging-message-batch-listener-with-headers-sub",
					topics = "plt-springMessagingMessageBatchListenerWithHeaders", batch = true)
			void springMessagingMessageBatchListenerWithHeaders(
					List<org.springframework.messaging.Message<String>> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {
				capturedBatchData = data.stream()
					.map(org.springframework.messaging.Message::getPayload)
					.collect(Collectors.toList());
				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				springMessagingMessageBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "plt-pulsarMessages-batch-listener-with-headers-sub",
					topics = "plt-pulsarMessagesBatchListenerWithHeaders", batch = true)
			void pulsarMessagesBatchListenerWithHeaders(Messages<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {
				List<String> list = new ArrayList<>();
				data.iterator().forEachRemaining(m -> list.add(m.getValue()));
				capturedBatchData = list;
				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				pulsarMessagesBatchListenerLatch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarHeadersCustomObjectMapperTestConfig.class)
	class PulsarHeadersCustomObjectMapperTest {

		private static final String TOPIC = "plt-listenerWithPojoHeaderCustom";

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
			MessageId messageId = pulsarTemplate.newMessage(msg)
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

			@PulsarListener(topics = TOPIC, subscriptionName = TOPIC + "-sub")
			void listenerWithPojoHeader(String ignored, @Header("user") UserRecord user) {
				userPassedIntoListener = user;
				listenerLatch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ConsumerPauseTest.ConsumerPauseConfig.class)
	class ConsumerPauseTest {

		private static final CountDownLatch latch = new CountDownLatch(10);

		@Autowired
		private PulsarListenerEndpointRegistry pulsarListenerEndpointRegistry;

		@Test
		void containerPauseAndResumeSuccessfully() throws Exception {
			for (int i = 0; i < 3; i++) {
				pulsarTemplate.send("consumer-pause-topic", "hello-" + i);
			}
			// wait until all 3 messages are received by the listener
			Awaitility.await().timeout(Duration.ofSeconds(10)).until(() -> latch.getCount() == 7);
			PulsarMessageListenerContainer container = pulsarListenerEndpointRegistry
				.getListenerContainer("consumerPauseListener");
			assertThat(container).isNotNull();
			container.pause();

			Thread.sleep(1000);
			for (int i = 3; i < 10; i++) {
				pulsarTemplate.send("consumer-pause-topic", "hello-" + i);
			}
			Thread.sleep(1000);
			assertThat(latch.getCount()).isEqualTo(7);

			container.resume();
			// All latch must be received by now
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class ConsumerPauseConfig {

			@PulsarListener(id = "consumerPauseListener", subscriptionName = "consumer-pause-subscription",
					topics = "consumer-pause-topic", properties = { "receiverQueueSize=1" })
			void listen(String msg) {
				latch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SubscriptionTypeTestsConfig.class)
	class SubscriptionTypeTests {

		static final CountDownLatch latchTypeNotSet = new CountDownLatch(1);

		static final CountDownLatch latchTypeSetOnAnnotation = new CountDownLatch(1);

		static final CountDownLatch latchTypeSetOnCustomizer = new CountDownLatch(1);

		@Test
		void defaultTypeFromContainerFactoryUsedWhenTypeNotSetAnywhere() throws Exception {
			pulsarTemplate.send("latchTypeNotSet-topic", "hello-latchTypeNotSet");
			assertThat(latchTypeNotSet.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void typeSetOnAnnotationOverridesDefaultTypeFromContainerFactory() throws Exception {
			pulsarTemplate.send("typeSetOnAnnotation-topic", "hello-typeSetOnAnnotation");
			assertThat(latchTypeSetOnAnnotation.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void typeSetOnCustomizerOverridesTypeSetOnAnnotation() throws Exception {
			pulsarTemplate.send("typeSetOnCustomizer-topic", "hello-typeSetOnCustomizer");
			assertThat(latchTypeSetOnCustomizer.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class SubscriptionTypeTestsConfig {

			@Bean
			ConsumerBuilderCustomizer<String> consumerFactoryCustomizerSubTypeIsIgnored() {
				return (b) -> b.subscriptionType(SubscriptionType.Shared);
			}

			@PulsarListener(topics = "latchTypeNotSet-topic", subscriptionName = "latchTypeNotSet-sub")
			void listenWithTypeNotSet(String ignored, Consumer<String> consumer) {
				assertSubscriptionType(consumer).isEqualTo(SubscriptionType.Exclusive);
				latchTypeNotSet.countDown();
			}

			@PulsarListener(topics = "typeSetOnAnnotation-topic", subscriptionName = "typeSetOnAnnotation-sub",
					subscriptionType = SubscriptionType.Key_Shared)
			void listenWithTypeSetOnAnnotation(String ignored, Consumer<String> consumer) {
				assertSubscriptionType(consumer).isEqualTo(SubscriptionType.Key_Shared);
				latchTypeSetOnAnnotation.countDown();
			}

			@PulsarListener(topics = "typeSetOnCustomizer-topic", subscriptionName = "typeSetOnCustomizer-sub",
					subscriptionType = SubscriptionType.Key_Shared, consumerCustomizer = "myCustomizer")
			void listenWithTypeSetOnCustomizer(String ignored, Consumer<String> consumer) {
				assertSubscriptionType(consumer).isEqualTo(SubscriptionType.Failover);
				latchTypeSetOnCustomizer.countDown();
			}

			@Bean
			public PulsarListenerConsumerBuilderCustomizer<String> myCustomizer() {
				return cb -> cb.subscriptionType(SubscriptionType.Failover);
			}

			@SuppressWarnings("rawtypes")
			private static AbstractObjectAssert<?, ?> assertSubscriptionType(Consumer<?> consumer) {
				return assertThat(consumer)
					.extracting("conf", InstanceOfAssertFactories.type(ConsumerConfigurationData.class))
					.extracting(ConsumerConfigurationData::getSubscriptionType);
			}

		}

	}

}

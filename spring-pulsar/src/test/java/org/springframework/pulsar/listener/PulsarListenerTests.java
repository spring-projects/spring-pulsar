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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTestContainerSupport;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 */
@SpringJUnitConfig
@DirtiesContext
public class PulsarListenerTests implements PulsarTestContainerSupport {

	static CountDownLatch latch = new CountDownLatch(1);
	static CountDownLatch latch1 = new CountDownLatch(3);
	static CountDownLatch latch2 = new CountDownLatch(3);

	@Autowired
	PulsarTemplate<String> pulsarTemplate;

	@Autowired
	private PulsarClient pulsarClient;

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	public static class TopLevelConfig {

		@Bean
		public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			Map<String, Object> config = new HashMap<>();
			config.put("topicName", "foo-1");
			return new DefaultPulsarProducerFactory<>(pulsarClient, config);
		}

		@Bean
		public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
			return new PulsarClientFactoryBean(pulsarClientConfiguration);
		}

		@Bean
		public PulsarClientConfiguration pulsarClientConfiguration() {
			return new PulsarClientConfiguration(Map.of("serviceUrl", PulsarTestContainerSupport.getPulsarBrokerUrl()));
		}

		@Bean
		public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			return new PulsarTemplate<>(pulsarProducerFactory);
		}

		@Bean
		public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient) {
			Map<String, Object> config = new HashMap<>();
			return new DefaultPulsarConsumerFactory<>(pulsarClient, config);
		}

		@Bean
		PulsarListenerContainerFactory pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory) {
			ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>(
					pulsarConsumerFactory, new PulsarContainerProperties(), null);
			return pulsarListenerContainerFactory;
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(
					PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl()));
		}

		@Bean
		PulsarTopic partitionedTopic() {
			return PulsarTopic.builder("persistent://public/default/concurrency-on-pl").numberOfPartitions(3).build();
		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerBasicTestCases.TestPulsarListenersForBasicScenario.class)
	class PulsarListenerBasicTestCases {

		@Test
		void testPulsarListenerWithTopicsPattern(@Autowired PulsarListenerEndpointRegistry registry) throws Exception {
			PulsarMessageListenerContainer baz = registry.getListenerContainer("baz");
			PulsarContainerProperties containerProperties = baz.getContainerProperties();
			assertThat(containerProperties.getTopicsPattern()).isEqualTo("persistent://public/default/pattern.*");

			pulsarTemplate.send("persistent://public/default/pattern-1", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-2", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-3", "hello baz");

			assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void testPulsarListenerProvidedConsumerProperties(@Autowired PulsarListenerEndpointRegistry registry)
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
			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Map.of("batchingEnabled", false));
			PulsarTemplate<String> customTemplate = new PulsarTemplate<>(pulsarProducerFactory);

			ConcurrentPulsarMessageListenerContainer<?> bar = (ConcurrentPulsarMessageListenerContainer<?>) registry
					.getListenerContainer("bar");

			assertThat(bar.getConcurrency()).isEqualTo(3);

			customTemplate.sendAsync("concurrency-on-pl", "hello john doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello alice doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello buzz doe");

			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void nonDefaultConcurrencySettingNotAllowedOnExclusiveSubscriptions(
				@Autowired PulsarListenerEndpointRegistry registry) throws Exception {
			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Map.of("batchingEnabled", false));
			PulsarTemplate<String> customTemplate = new PulsarTemplate<>(pulsarProducerFactory);

			ConcurrentPulsarMessageListenerContainer<?> bar = (ConcurrentPulsarMessageListenerContainer<?>) registry
					.getListenerContainer("bar");

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
			void listen1(String message) {
				latch.countDown();
			}

			@PulsarListener(id = "bar", topics = "concurrency-on-pl", subscriptionName = "subscription-2",
					subscriptionType = SubscriptionType.Failover, concurrency = "3")
			void listen2(String message) {
				latch1.countDown();
			}

			@PulsarListener(id = "baz", topicPattern = "persistent://public/default/pattern.*",
					subscriptionName = "subscription-3",
					properties = { "patternAutoDiscoveryPeriod=5", "subscriptionInitialPosition=Earliest" })
			void listen3(String message) {
				latch2.countDown();
			}

			@PulsarListener(id = "ackMode-test-id", subscriptionName = "ackModeTest-sub", topics = "ackModeTest-topic",
					ackMode = AckMode.RECORD)
			void ackModeTestListener(String message) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = NegativeAckRedeliveryBackoffTest.NegativeAckRedeliveryConfig.class)
	class NegativeAckRedeliveryBackoffTest {

		static CountDownLatch nackRedeliveryBackoffLatch = new CountDownLatch(5);

		@Test
		void pulsarListenerWithNackRedeliveryBackoff(@Autowired PulsarListenerEndpointRegistry registry)
				throws Exception {
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
				return MultiplierRedeliveryBackoff.builder().minDelayMs(1000).maxDelayMs(5 * 1000).multiplier(2)
						.build();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = AckTimeoutkRedeliveryBackoffTest.AckTimeoutRedeliveryConfig.class)
	class AckTimeoutkRedeliveryBackoffTest {

		static CountDownLatch ackTimeoutRedeliveryBackoffLatch = new CountDownLatch(5);

		@Test
		void pulsarListenerWithAckTimeoutRedeliveryBackoff(@Autowired PulsarListenerEndpointRegistry registry)
				throws Exception {
			pulsarTemplate.send("withAckTimeoutRedeliveryBackoff-test-topic", "hello john doe");
			assertThat(ackTimeoutRedeliveryBackoffLatch.await(60, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class AckTimeoutRedeliveryConfig {

			@PulsarListener(id = "withAckTimeoutRedeliveryBackoff",
					subscriptionName = "withAckTimeoutRedeliveryBackoffSubscription",
					topics = "withAckTimeoutRedeliveryBackoff-test-topic",
					ackTimeoutRedeliveryBackoff = "ackTimeoutRedeliveryBackoff",
					subscriptionType = SubscriptionType.Shared, properties = { "ackTimeoutMillis=1000" })
			void listen(String msg) {
				ackTimeoutRedeliveryBackoffLatch.countDown();
				throw new RuntimeException();
			}

			@Bean
			public RedeliveryBackoff ackTimeoutRedeliveryBackoff() {
				return MultiplierRedeliveryBackoff.builder().minDelayMs(1000).maxDelayMs(5 * 1000).multiplier(2)
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
					subscriptionType = SubscriptionType.Shared, properties = { "ackTimeoutMillis=1000" })
			void listen(String msg) {
				latch.countDown();
				throw new RuntimeException("fail " + msg);
			}

			@PulsarListener(id = "dlqListener", topics = "dlpt-dlq-topic")
			void listenDlq(String msg) {
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
							.rootCause().isInstanceOf(IllegalStateException.class)
							.hasMessage("concurrency > 1 is not allowed on Exclusive subscription type");
		}

		@EnablePulsar
		static class ConcurrencyConfig {

			@PulsarListener(id = "foobar", topics = "concurrency-on-pl", subscriptionName = "subscription-3",
					concurrency = "3")
			void listen3(String message) {
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
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(JSONSchema.of(User.class));

			for (int i = 0; i < 3; i++) {
				template.send("json-topic", new User("Jason", i));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(jsonBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(AvroSchema.of(User.class));

			for (int i = 0; i < 3; i++) {
				template.send("avro-topic", new User("Avi", i));
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(avroBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, Integer>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, Collections.emptyMap());
			PulsarTemplate<KeyValue<String, Integer>> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<KeyValue<String, Integer>> kvSchema = Schema.KeyValue(Schema.STRING, Schema.INT32,
					KeyValueEncodingType.INLINE);
			template.setSchema(kvSchema);

			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-topic", new KeyValue<>("Kevin", i));
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(keyvalueBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<Proto.Person> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(ProtobufSchema.of(Proto.Person.class));

			for (int i = 0; i < 3; i++) {
				template.send("protobuf-topic", Proto.Person.newBuilder().setId(i).setName("Paul").build());
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(protobufBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class SchemaTestConfig {

			@PulsarListener(id = "jsonListener", topics = "json-topic", subscriptionName = "subscription-4",
					schemaType = SchemaType.JSON, properties = { "subscriptionInitialPosition=Earliest" })
			void listenJson(User message) {
				jsonLatch.countDown();
			}

			@PulsarListener(id = "jsonBatchListener", topics = "json-topic", subscriptionName = "subscription-5",
					schemaType = SchemaType.JSON, batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			void listenJsonBatch(List<User> messages) {
				messages.forEach(m -> jsonBatchLatch.countDown());
			}

			@PulsarListener(id = "avroListener", topics = "avro-topic", subscriptionName = "subscription-6",
					schemaType = SchemaType.AVRO, properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvro(User message) {
				avroLatch.countDown();
			}

			@PulsarListener(id = "avroBatchListener", topics = "avro-topic", subscriptionName = "subscription-7",
					schemaType = SchemaType.AVRO, batch = true, properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvroBatch(Messages<User> messages) {
				messages.forEach(m -> avroBatchLatch.countDown());
			}

			@PulsarListener(id = "keyvalueListener", topics = "keyvalue-topic", subscriptionName = "subscription-8",
					schemaType = SchemaType.KEY_VALUE, properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalue(KeyValue<String, Integer> message) {
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
			void listenProtobuf(Proto.Person message) {
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

	/**
	 * Do not convert this to a Record as Avro does not seem to work well w/ records.
	 */
	static class User {

		private String name;

		private int age;

		User() {
		}

		User(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			User user = (User) o;
			return age == user.age && Objects.equals(name, user.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age);
		}

		@Override
		public String toString() {
			return "User{" + "name='" + name + '\'' + ", age=" + age + '}';
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
			PulsarProducerFactory<User2> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User2> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(Schema.JSON(User2.class));

			for (int i = 0; i < 3; i++) {
				template.send("json-custom-mappings-topic", new User2("Jason", i));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(AvroSchema.of(User.class));

			for (int i = 0; i < 3; i++) {
				template.send("avro-custom-mappings-topic", new User("Avi", i));
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, User2>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, Collections.emptyMap());
			PulsarTemplate<KeyValue<String, User2>> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<KeyValue<String, User2>> kvSchema = Schema.KeyValue(Schema.STRING, Schema.JSON(User2.class),
					KeyValueEncodingType.INLINE);
			template.setSchema(kvSchema);

			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-custom-mappings-topic", new KeyValue<>("Kevin", new User2("Kevin", 5150)));
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<Proto.Person> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(ProtobufSchema.of(Proto.Person.class));

			for (int i = 0; i < 3; i++) {
				template.send("protobuf-custom-mappings-topic",
						Proto.Person.newBuilder().setId(i).setName("Paul").build());
			}
			assertThat(protobufLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class SchemaCustomMappingsTestConfig {

			@Bean
			SchemaResolver customSchemaResolver() {
				Map<Class<?>, Schema<?>> customMappings = new HashMap<>();
				customMappings.put(User.class, Schema.AVRO(User.class));
				customMappings.put(User2.class, Schema.JSON(User2.class));
				customMappings.put(Proto.Person.class, Schema.PROTOBUF(Proto.Person.class));
				return new DefaultSchemaResolver(customMappings);
			}

			@Bean
			PulsarListenerContainerFactory pulsarListenerContainerFactory(
					PulsarConsumerFactory<Object> pulsarConsumerFactory, SchemaResolver schemaResolver) {
				PulsarContainerProperties containerProps = new PulsarContainerProperties();
				containerProps.setSchemaResolver(schemaResolver);
				ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>(
						pulsarConsumerFactory, containerProps, null);
				return pulsarListenerContainerFactory;
			}

			@PulsarListener(id = "jsonListener", topics = "json-custom-mappings-topic",
					subscriptionName = "subscription-4", properties = { "subscriptionInitialPosition=Earliest" })
			void listenJson(User2 message) {
				jsonLatch.countDown();
			}

			@PulsarListener(id = "avroListener", topics = "avro-custom-mappings-topic",
					subscriptionName = "subscription-6", properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvro(User message) {
				avroLatch.countDown();
			}

			@PulsarListener(id = "keyvalueListener", topics = "keyvalue-custom-mappings-topic",
					subscriptionName = "subscription-8", properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalue(KeyValue<String, User2> message) {
				keyvalueLatch.countDown();
			}

			@PulsarListener(id = "protobufListener", topics = "protobuf-custom-mappings-topic",
					subscriptionName = "subscription-10", properties = { "subscriptionInitialPosition=Earliest" })
			void listenProtobuf(Proto.Person message) {
				protobufLatch.countDown();
			}

		}

		record User2(String name, int age) {
		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerTests.PulsarHeadersTest.PulsarListerWithHeadersConfig.class)
	class PulsarHeadersTest {

		static CountDownLatch simpleListenerLatch = new CountDownLatch(1);
		static CountDownLatch pulsarMessageListenerLatch = new CountDownLatch(1);
		static CountDownLatch springMessagingMessageListenerLatch = new CountDownLatch(1);

		static volatile String capturedData;
		static volatile MessageId messageId;
		static volatile String topicName;
		static volatile String fooValue;
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
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "simpleListenerWithHeaders"))
					.withTopic("simpleListenerWithHeaders").send();
			assertThat(simpleListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-simple-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/simpleListenerWithHeaders");
			assertThat(fooValue).isEqualTo("simpleListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-simple-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void pulsarMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "pulsarMessageListenerWithHeaders"))
					.withTopic("pulsarMessageListenerWithHeaders").send();
			assertThat(pulsarMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-pulsar-message-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/pulsarMessageListenerWithHeaders");
			assertThat(fooValue).isEqualTo("pulsarMessageListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-pulsar-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void springMessagingMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-listener")
					.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo",
							"springMessagingMessageListenerWithHeaders"))
					.withTopic("springMessagingMessageListenerWithHeaders").send();
			assertThat(springMessagingMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData).isEqualTo("hello-spring-messaging-message-listener");
			assertThat(PulsarHeadersTest.messageId).isEqualTo(messageId);
			assertThat(topicName).isEqualTo("persistent://public/default/springMessagingMessageListenerWithHeaders");
			assertThat(fooValue).isEqualTo("springMessagingMessageListenerWithHeaders");
			assertThat(rawData).isEqualTo("hello-spring-messaging-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void simpleBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-simple-batch-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "simpleBatchListenerWithHeaders"))
					.withTopic("simpleBatchListenerWithHeaders").send();
			assertThat(simpleBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-simple-batch-listener");
			assertThat(batchMessageIds).containsExactly(messageId);
			assertThat(batchTopicNames).containsExactly("persistent://public/default/simpleBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("simpleBatchListenerWithHeaders");
		}

		@Test
		void pulsarMessageBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-batch-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "pulsarMessageBatchListenerWithHeaders"))
					.withTopic("pulsarMessageBatchListenerWithHeaders").send();
			assertThat(pulsarMessageBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-pulsar-message-batch-listener");
			assertThat(batchTopicNames)
					.containsExactly("persistent://public/default/pulsarMessageBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("pulsarMessageBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@Test
		void springMessagingMessageBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-batch-listener")
					.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo",
							"springMessagingMessageBatchListenerWithHeaders"))
					.withTopic("springMessagingMessageBatchListenerWithHeaders").send();
			assertThat(springMessagingMessageBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-spring-messaging-message-batch-listener");
			assertThat(batchTopicNames)
					.containsExactly("persistent://public/default/springMessagingMessageBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("springMessagingMessageBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@Test
		void pulsarMessagesBatchListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-messages-batch-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "pulsarMessagesBatchListenerWithHeaders"))
					.withTopic("pulsarMessagesBatchListenerWithHeaders").send();
			assertThat(pulsarMessagesBatchListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedBatchData).containsExactly("hello-pulsar-messages-batch-listener");
			assertThat(batchTopicNames)
					.containsExactly("persistent://public/default/pulsarMessagesBatchListenerWithHeaders");
			assertThat(batchFooValues).containsExactly("pulsarMessagesBatchListenerWithHeaders");
			assertThat(batchMessageIds).containsExactly(messageId);
		}

		@EnablePulsar
		@Configuration
		static class PulsarListerWithHeadersConfig {

			@PulsarListener(subscriptionName = "simple-listener-with-headers-sub", topics = "simpleListenerWithHeaders")
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

			@PulsarListener(subscriptionName = "pulsar-message-listener-with-headers-sub",
					topics = "pulsarMessageListenerWithHeaders")
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

			@PulsarListener(subscriptionName = "pulsar-message-listener-with-headers-sub",
					topics = "springMessagingMessageListenerWithHeaders")
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

			@PulsarListener(subscriptionName = "simple-batch-listener-with-headers-sub",
					topics = "simpleBatchListenerWithHeaders", batch = true)
			void simpleBatchListenerWithHeaders(List<String> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {
				capturedBatchData = data;
				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				simpleBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "pulsarMessage-batch-listener-with-headers-sub",
					topics = "pulsarMessageBatchListenerWithHeaders", batch = true)
			void pulsarMessageBatchListenerWithHeaders(List<Message<String>> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {

				capturedBatchData = data.stream().map(Message::getValue).collect(Collectors.toList());

				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				pulsarMessageBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "spring-messaging-message-batch-listener-with-headers-sub",
					topics = "springMessagingMessageBatchListenerWithHeaders", batch = true)
			void springMessagingMessageBatchListenerWithHeaders(
					List<org.springframework.messaging.Message<String>> data,
					@Header(PulsarHeaders.MESSAGE_ID) List<MessageId> messageIds,
					@Header(PulsarHeaders.TOPIC_NAME) List<String> topicNames, @Header("foo") List<String> fooValues) {

				capturedBatchData = data.stream().map(org.springframework.messaging.Message::getPayload)
						.collect(Collectors.toList());

				batchMessageIds = messageIds;
				batchTopicNames = topicNames;
				batchFooValues = fooValues;
				springMessagingMessageBatchListenerLatch.countDown();
			}

			@PulsarListener(subscriptionName = "pulsarMessages-batch-listener-with-headers-sub",
					topics = "pulsarMessagesBatchListenerWithHeaders", batch = true)
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

}

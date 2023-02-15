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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.EnableReactivePulsar;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.SchemaCustomMappingsTestCases.SchemaCustomMappingsTestConfig.User2;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link ReactivePulsarListener} annotation.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
public class ReactivePulsarListenerTests implements PulsarTestContainerSupport {

	@Autowired
	PulsarTemplate<String> pulsarTemplate;

	@Autowired
	private PulsarClient pulsarClient;

	@Configuration(proxyBeanMethods = false)
	@EnableReactivePulsar
	public static class TopLevelConfig {

		@Bean
		public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarProducerFactory<>(pulsarClient, new HashMap<>());
		}

		@Bean
		public PulsarClientFactoryBean pulsarClientFactoryBean() {
			return new PulsarClientFactoryBean(Map.of("serviceUrl", PulsarTestContainerSupport.getPulsarBrokerUrl()));
		}

		@Bean
		public ReactivePulsarClient pulsarReactivePulsarClient(PulsarClient pulsarClient) {
			return AdaptedReactivePulsarClientFactory.create(pulsarClient);
		}

		@Bean
		public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			return new PulsarTemplate<>(pulsarProducerFactory);
		}

		@Bean
		public ReactivePulsarConsumerFactory<String> pulsarConsumerFactory(ReactivePulsarClient pulsarClient) {
			return new DefaultReactivePulsarConsumerFactory<>(pulsarClient, new MutableReactiveMessageConsumerSpec());
		}

		@Bean
		ReactivePulsarListenerContainerFactory<String> reactivePulsarListenerContainerFactory(
				ReactivePulsarConsumerFactory<String> pulsarConsumerFactory) {
			return new DefaultReactivePulsarListenerContainerFactory<>(pulsarConsumerFactory,
					new ReactivePulsarContainerProperties<>());
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

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<?> subscriptionInitialPositionEarliest() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerBasicTestCases.TestPulsarListenersForBasicScenario.class)
	class PulsarListenerBasicTestCases {

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

			pulsarTemplate.send("persistent://public/default/pattern-1", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-2", "hello baz");
			pulsarTemplate.send("persistent://public/default/pattern-3", "hello baz");

			assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnableReactivePulsar
		@Configuration
		static class TestPulsarListenersForBasicScenario {

			@ReactivePulsarListener(id = "id-1", topics = "topic-1", subscriptionName = "subscription-1",
					consumerCustomizer = "consumerCustomizer")
			Mono<Void> listen1(String ignored) {
				latch1.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(consumerCustomizer = "listen2Customizer")
			Mono<Void> listen2(String ignored) {
				latch2.countDown();
				return Mono.empty();
			}

			@Bean
			ReactiveMessageConsumerBuilderCustomizer<String> listen2Customizer() {
				return b -> b.topics(List.of("topic-2"))
						.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

			@ReactivePulsarListener(id = "id-3", topicPattern = "persistent://public/default/pattern.*",
					subscriptionName = "subscription-3", consumerCustomizer = "consumerCustomizer")
			Mono<Void> listen3(String ignored) {
				latch3.countDown();
				return Mono.empty();
			}

			@Bean
			ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
				return b -> b.topicsPatternAutoDiscoveryPeriod(Duration.ofSeconds(2))
						.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PulsarListenerStreamingTestCases.TestPulsarListenersForStreaming.class)
	class PulsarListenerStreamingTestCases {

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
		static class TestPulsarListenersForStreaming {

			@ReactivePulsarListener(topics = "streaming-1", stream = true,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listen1(Flux<Message<String>> messages) {
				return messages.doOnNext(m -> latch1.countDown()).map(MessageResult::acknowledge);
			}

			@ReactivePulsarListener(topics = "streaming-2", stream = true,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Flux<MessageResult<Void>> listen2(Flux<org.springframework.messaging.Message<String>> messages) {
				return messages.doOnNext(m -> latch2.countDown()).map(m -> {
					Object mId = m.getHeaders().get(PulsarHeaders.MESSAGE_ID);
					if (mId instanceof MessageId) {
						return (MessageId) mId;
					}
					else {
						throw new RuntimeException("Missing message Id");
					}
				}).map(MessageResult::acknowledge);
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
			ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
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
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("json-topic", new User("Jason", i), JSONSchema.of(User.class));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("avro-topic", new User("Avi", i), AvroSchema.of(User.class));
			}
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, Integer>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, Collections.emptyMap());
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
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
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
			Mono<Void> listenJson(User ignored) {
				jsonLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "avroListener", topics = "avro-topic", schemaType = SchemaType.AVRO,
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenAvro(User ignored) {
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
			for (int i = 0; i < 3; i++) {
				template.send("json-custom-schema-topic", new User2("Jason", i), JSONSchema.of(User2.class));
			}
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			for (int i = 0; i < 3; i++) {
				template.send("avro-custom-schema-topic", new User("Avi", i), AvroSchema.of(User.class));
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
			for (int i = 0; i < 3; i++) {
				template.send("keyvalue-custom-schema-topic", new KeyValue<>("Kevin", new User2("Kevin", 5150)),
						kvSchema);
			}
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void protobufSchema() throws Exception {
			PulsarProducerFactory<Proto.Person> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
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
				resolver.addCustomSchemaMapping(User.class, Schema.AVRO(User.class));
				resolver.addCustomSchemaMapping(User2.class, Schema.JSON(User2.class));
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
			Mono<Void> listenJson(User2 ignored) {
				jsonLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "avroListener", topics = "avro-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenAvro(User ignored) {
				avroLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "keyvalueListener", topics = "keyvalue-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenKeyvalue(KeyValue<String, User2> ignored) {
				keyvalueLatch.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "protobufListener", topics = "protobuf-custom-schema-topic",
					consumerCustomizer = "subscriptionInitialPositionEarliest")
			Mono<Void> listenProtobuf(Proto.Person ignored) {
				protobufLatch.countDown();
				return Mono.empty();
			}

			record User2(String name, int age) {
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

	}

	@Nested
	@ContextConfiguration(classes = TopicCustomMappingsTestCases.TopicCustomMappingsTestConfig.class)
	class TopicCustomMappingsTestCases {

		static CountDownLatch userLatch = new CountDownLatch(3);
		static CountDownLatch stringLatch = new CountDownLatch(3);

		@Test
		void complexMessageTypeTopicMapping() throws Exception {
			PulsarProducerFactory<User2> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User2> template = new PulsarTemplate<>(pulsarProducerFactory);
			Schema<User2> schema = Schema.JSON(User2.class);
			for (int i = 0; i < 3; i++) {
				template.send("rplt-topicMapping-user-topic", new User2("Jason", i), schema);
			}
			assertThat(userLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void primitiveMessageTypeTopicMapping() throws Exception {
			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
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
				resolver.addCustomTopicMapping(User2.class, "rplt-topicMapping-user-topic");
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
			Mono<Void> listenUser(User2 ignored) {
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
	@ContextConfiguration(classes = ReactivePulsarListenerTests.PulsarHeadersTest.PulsarListenerWithHeadersConfig.class)
	class PulsarHeadersTest {

		static CountDownLatch simpleListenerLatch = new CountDownLatch(1);
		static CountDownLatch pulsarMessageListenerLatch = new CountDownLatch(1);
		static CountDownLatch springMessagingMessageListenerLatch = new CountDownLatch(1);

		static AtomicReference<String> capturedData = new AtomicReference<>();
		static AtomicReference<MessageId> messageId = new AtomicReference<>();
		static AtomicReference<String> topicName = new AtomicReference<>();
		static AtomicReference<String> fooValue = new AtomicReference<>();
		static AtomicReference<byte[]> rawData = new AtomicReference<>();

		@Test
		void simpleListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-simple-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "simpleListenerWithHeaders"))
					.withTopic("simpleListenerWithHeaders").send();
			assertThat(simpleListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData.get()).isEqualTo("hello-simple-listener");
			assertThat(PulsarHeadersTest.messageId.get()).isEqualTo(messageId);
			assertThat(topicName.get()).isEqualTo("persistent://public/default/simpleListenerWithHeaders");
			assertThat(fooValue.get()).isEqualTo("simpleListenerWithHeaders");
			assertThat(rawData.get()).isEqualTo("hello-simple-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void pulsarMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-pulsar-message-listener")
					.withMessageCustomizer(
							messageBuilder -> messageBuilder.property("foo", "pulsarMessageListenerWithHeaders"))
					.withTopic("pulsarMessageListenerWithHeaders").send();
			assertThat(pulsarMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData.get()).isEqualTo("hello-pulsar-message-listener");
			assertThat(PulsarHeadersTest.messageId.get()).isEqualTo(messageId);
			assertThat(topicName.get()).isEqualTo("persistent://public/default/pulsarMessageListenerWithHeaders");
			assertThat(fooValue.get()).isEqualTo("pulsarMessageListenerWithHeaders");
			assertThat(rawData.get()).isEqualTo("hello-pulsar-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@Test
		void springMessagingMessageListenerWithHeaders() throws Exception {
			MessageId messageId = pulsarTemplate.newMessage("hello-spring-messaging-message-listener")
					.withMessageCustomizer(messageBuilder -> messageBuilder.property("foo",
							"springMessagingMessageListenerWithHeaders"))
					.withTopic("springMessagingMessageListenerWithHeaders").send();
			assertThat(springMessagingMessageListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(capturedData.get()).isEqualTo("hello-spring-messaging-message-listener");
			assertThat(PulsarHeadersTest.messageId.get()).isEqualTo(messageId);
			assertThat(topicName.get())
					.isEqualTo("persistent://public/default/springMessagingMessageListenerWithHeaders");
			assertThat(fooValue.get()).isEqualTo("springMessagingMessageListenerWithHeaders");
			assertThat(rawData.get())
					.isEqualTo("hello-spring-messaging-message-listener".getBytes(StandardCharsets.UTF_8));
		}

		@EnableReactivePulsar
		@Configuration
		static class PulsarListenerWithHeadersConfig {

			@ReactivePulsarListener(subscriptionName = "simple-listener-with-headers-sub",
					topics = "simpleListenerWithHeaders", consumerCustomizer = "subscriptionInitialPositionEarliest")
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

			@ReactivePulsarListener(subscriptionName = "pulsar-message-listener-with-headers-sub",
					topics = "pulsarMessageListenerWithHeaders",
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

			@ReactivePulsarListener(subscriptionName = "pulsar-message-listener-with-headers-sub",
					topics = "springMessagingMessageListenerWithHeaders",
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
			pulsarTemplate.newMessage("first").withTopic("pulsarListenerWithConcurrencyKeyOrdered")
					.withMessageCustomizer(m -> m.key("key")).send();
			pulsarTemplate.newMessage("second").withTopic("pulsarListenerWithConcurrencyKeyOrdered")
					.withMessageCustomizer(m -> m.key("key")).send();
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

}

/*
 * Copyright 2022 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.AbstractContainerBaseTests;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Soby Chacko
 * @author Alexander Preuß
 */
@SpringJUnitConfig
@DirtiesContext
public class PulsarListenerTests extends AbstractContainerBaseTests {

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
			return new PulsarClientConfiguration(Map.of("serviceUrl", getPulsarBrokerUrl()));
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
		PulsarListenerContainerFactory<?> pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory) {
			final ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>();
			pulsarListenerContainerFactory.setPulsarConsumerFactory(pulsarConsumerFactory);
			return pulsarListenerContainerFactory;
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()));
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

			final PulsarContainerProperties pulsarContainerProperties = registry.getListenerContainer("foo")
					.getContainerProperties();
			final Properties pulsarConsumerProperties = pulsarContainerProperties.getPulsarConsumerProperties();
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

			final ConcurrentPulsarMessageListenerContainer<?> bar = (ConcurrentPulsarMessageListenerContainer<?>) registry
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

			final ConcurrentPulsarMessageListenerContainer<?> bar = (ConcurrentPulsarMessageListenerContainer<?>) registry
					.getListenerContainer("bar");

			assertThat(bar.getConcurrency()).isEqualTo(3);

			customTemplate.sendAsync("concurrency-on-pl", "hello john doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello alice doe");
			customTemplate.sendAsync("concurrency-on-pl", "hello buzz doe");

			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class TestPulsarListenersForBasicScenario {

			@PulsarListener(id = "foo", properties = { "subscriptionName=subscription-1", "topicNames=foo-1" })
			void listen1(String message) {
				latch.countDown();
			}

			@PulsarListener(id = "bar", topics = "concurrency-on-pl", subscriptionName = "subscription-2",
					subscriptionType = "failover", concurrency = "3")
			void listen2(String message) {
				latch1.countDown();
			}

			@PulsarListener(id = "baz", topicPattern = "persistent://public/default/pattern.*",
					subscriptionName = "subscription-3",
					properties = { "patternAutoDiscoveryPeriod=5", "subscriptionInitialPosition=Earliest" })
			void listen3(String message) {
				latch2.countDown();
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

		static CountDownLatch jsonLatch = new CountDownLatch(1);

		static CountDownLatch avroLatch = new CountDownLatch(1);

		static CountDownLatch keyvalueLatch = new CountDownLatch(1);



		@Test
		void jsonSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(JSONSchema.of(User.class));
			template.send("json-topic", new User("Jason", 1));
			assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void avroSchema() throws Exception {
			PulsarProducerFactory<User> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					Collections.emptyMap());
			PulsarTemplate<User> template = new PulsarTemplate<>(pulsarProducerFactory);
			template.setSchema(AvroSchema.of(User.class));
			template.send("avro-topic", new User("Avi", 2));
			assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void keyvalueSchema() throws Exception {
			PulsarProducerFactory<KeyValue<String, Integer>> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, Collections.emptyMap());
			PulsarTemplate<KeyValue<String, Integer>> template = new PulsarTemplate<>(pulsarProducerFactory);

			Schema<KeyValue<String, Integer>> kvSchema = Schema.KeyValue(Schema.STRING, Schema.INT32,
					KeyValueEncodingType.INLINE);

			template.setSchema(kvSchema);
			template.send("keyvalue-topic", new KeyValue<>("Kevin", 3));
			assertThat(keyvalueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class SchemaTestConfig {

			@PulsarListener(id = "jsonListener", topics = "json-topic", subscriptionName = "subscription-4",
					schemaType = SchemaType.JSON, properties = { "subscriptionInitialPosition=Earliest" })
			void listenJson(User message) {
				jsonLatch.countDown();
			}

			@PulsarListener(id = "avroListener", topics = "avro-topic", subscriptionName = "subscription-5",
					schemaType = SchemaType.AVRO, properties = { "subscriptionInitialPosition=Earliest" })
			void listenAvro(User message) {
				avroLatch.countDown();
			}

			@PulsarListener(id = "keyvalueListener", topics = "keyvalue-topic", subscriptionName = "subscription-6",
					schemaType = SchemaType.KEY_VALUE, properties = { "subscriptionInitialPosition=Earliest" })
			void listenKeyvalue(KeyValue<String, Integer> message) {
				keyvalueLatch.countDown();
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

}

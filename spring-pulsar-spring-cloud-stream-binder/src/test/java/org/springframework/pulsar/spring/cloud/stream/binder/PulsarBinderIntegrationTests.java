/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.autoconfigure.PulsarProperties;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.SchemaResolver.SchemaResolverCustomizer;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Integration tests for {@link PulsarBinderIntegrationTests}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@ExtendWith(OutputCaptureExtension.class)
@SuppressWarnings("JUnitMalformedDeclaration")
class PulsarBinderIntegrationTests implements PulsarTestContainerSupport {

	private static final int AWAIT_DURATION = 10;

	@Test
	void binderAndBindingPropsAreAppliedAndRespected(CapturedOutput output) {
		SpringApplication app = new SpringApplication(BinderAndBindingPropsTestConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
				"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
				"--spring.cloud.function.definition=textSupplier;textLogger",
				"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
				"--spring.pulsar.producer.producer-name=textSupplierProducer-fromBase",
				"--spring.cloud.stream.pulsar.binder.producer.producer-name=textSupplierProducer-fromBinder",
				"--spring.cloud.stream.pulsar.bindings.textSupplier-out-0.producer.producer-name=textSupplierProducer-fromBinding",
				"--spring.cloud.stream.pulsar.binder.producer.max-pending-messages=1100",
				"--spring.pulsar.producer.block-if-queue-full=true",
				"--spring.cloud.stream.pulsar.binder.consumer.subscription-name=textLoggerSub-fromBinder",
				"--spring.cloud.stream.pulsar.binder.consumer.consumer-name=textLogger-fromBinder",
				"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.consumer-name=textLogger-fromBinding")) {

			Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
					.until(() -> output.toString().contains("Hello binder: test-basic-scenario"));

			// now verify the properties were set onto producer and consumer as expected
			TrackingProducerFactory producerFactory = context.getBean(TrackingProducerFactory.class);
			assertThat(producerFactory.producersCreated).isNotEmpty().element(0)
					.hasFieldOrPropertyWithValue("producerName", "textSupplierProducer-fromBinding")
					.hasFieldOrPropertyWithValue("conf.maxPendingMessages", 1100)
					.hasFieldOrPropertyWithValue("conf.blockIfQueueFull", true);

			TrackingConsumerFactory consumerFactory = context.getBean(TrackingConsumerFactory.class);
			assertThat(consumerFactory.consumersCreated).isNotEmpty().element(0)
					.hasFieldOrPropertyWithValue("consumerName", "textLogger-fromBinding")
					.hasFieldOrPropertyWithValue("conf.subscriptionName", "textLoggerSub-fromBinder");
		}
	}

	@Nested
	class DefaultEncoding {

		@Test
		void primitiveTypeString(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveTextConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=textSupplier;textLogger",
					"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
					"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.subscription-name=pbit-text-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: test-basic-scenario"));
			}
		}

		@Test
		void primitiveTypeFloat(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveFloatConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piSupplier-out-0.destination=pi-stream",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=pi-stream",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription-name=pbit-float-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 3.14"));
			}
		}

	}

	@Nested
	class NativeEncoding {

		@Test
		void primitiveTypeFloat(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveFloatConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=piSupplier-out-0",
					"--spring.cloud.stream.bindings.piSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.piLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.schema-type=FLOAT",
					"--spring.cloud.stream.pulsar.bindings.piSupplier-out-0.producer.schema-type=FLOAT",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription-name=pbit-float-sub2")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 3.14"));
			}
		}

		@Test
		void jsonTypeFoo(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=fooSupplier-out-0",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.schema-type=JSON",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub1",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.schema-type=JSON",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void jsonTypeFooWithNoSchemaTypeAndCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooWithCustomMappingConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub2",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void jsonTypeFooWithNoSchemaTypeAndNoCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-3",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-3",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub3",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION)).until(
						() -> output.toString().contains("Could not determine producer schema for foo-stream-3"));
			}
		}

		@Test
		void jsonTypeFooConsumerWithNoSchemaTypeAndNoCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConsumerConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-4",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-4",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub4")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION)).until(
						() -> output.toString().contains("Could not determine consumer schema for foo-stream-4"));
			}
		}

		@Test
		void keyValueTypeWithCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=kv-stream-1",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=kv-stream-1",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-value-type="
							+ Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-kv-sub1",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-value-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 5150->Foo[value=5150]"));
			}
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class PrimitiveTextConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<String> textSupplier() {
			return () -> "test-basic-scenario";
		}

		@Bean
		public Consumer<String> textLogger() {
			return s -> this.logger.info("Hello binder: " + s);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(PrimitiveTextConfig.class)
	static class BinderAndBindingPropsTestConfig {

		@Bean
		public PulsarProducerFactory<?> pulsarProducerFactory(PulsarClient pulsarClient,
				PulsarProperties pulsarProperties, TopicResolver topicResolver) {
			return new TrackingProducerFactory(pulsarClient, pulsarProperties.buildProducerProperties(), topicResolver);
		}

		@Bean
		public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient,
				PulsarProperties pulsarProperties) {
			return new TrackingConsumerFactory(pulsarClient, pulsarProperties.buildConsumerProperties());
		}

	}

	static class TrackingProducerFactory extends DefaultPulsarProducerFactory<String> {

		List<Producer<String>> producersCreated = new ArrayList<>();

		TrackingProducerFactory(PulsarClient pulsarClient, Map<String, Object> config, TopicResolver topicResolver) {
			super(pulsarClient, config, topicResolver);
		}

		@Override
		protected Producer<String> doCreateProducer(Schema<String> schema, @Nullable String topic,
				@Nullable Collection<String> encryptionKeys,
				@Nullable List<ProducerBuilderCustomizer<String>> producerBuilderCustomizers)
				throws PulsarClientException {
			Producer<String> producer = super.doCreateProducer(schema, topic, encryptionKeys,
					producerBuilderCustomizers);
			producersCreated.add(producer);
			return producer;
		}

	}

	static class TrackingConsumerFactory extends DefaultPulsarConsumerFactory<String> {

		List<org.apache.pulsar.client.api.Consumer<String>> consumersCreated = new ArrayList<>();

		TrackingConsumerFactory(PulsarClient pulsarClient, Map<String, Object> consumerConfig) {
			super(pulsarClient, consumerConfig);
		}

		@Override
		public org.apache.pulsar.client.api.Consumer<String> createConsumer(Schema<String> schema,
				@Nullable Collection<String> topics, @Nullable String subscriptionName,
				@Nullable Map<String, String> metadataProperties,
				@Nullable List<ConsumerBuilderCustomizer<String>> consumerBuilderCustomizers)
				throws PulsarClientException {
			org.apache.pulsar.client.api.Consumer<String> consumer = super.createConsumer(schema, topics,
					subscriptionName, metadataProperties, consumerBuilderCustomizers);
			consumersCreated.add(consumer);
			return consumer;
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class PrimitiveFloatConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<Float> piSupplier() {
			return () -> 3.14f;
		}

		@Bean
		public Consumer<Float> piLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class JsonFooConsumerConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		public Supplier<String> fooSupplier() {
			return () -> "5150";
		}

		@Bean
		public Consumer<Foo> fooLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class JsonFooConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<Foo> fooSupplier() {
			return () -> new Foo("5150");
		}

		@Bean
		public Consumer<Foo> fooLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(JsonFooConfig.class)
	static class JsonFooWithCustomMappingConfig {

		@Bean
		public SchemaResolverCustomizer<DefaultSchemaResolver> customMappings() {
			return (resolver) -> resolver.addCustomSchemaMapping(Foo.class, JSONSchema.of(Foo.class));
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class KeyValueFooConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public SchemaResolverCustomizer<DefaultSchemaResolver> customMappings() {
			return (resolver) -> resolver.addCustomSchemaMapping(Foo.class, JSONSchema.of(Foo.class));
		}

		@Bean
		public Supplier<KeyValue<String, Foo>> fooSupplier() {
			return () -> new KeyValue<>("5150", new Foo("5150"));
		}

		@Bean
		public Consumer<KeyValue<String, Foo>> fooLogger() {
			return f -> this.logger.info("Hello binder: " + f.getKey() + "->" + f.getValue());
		}

	}

	record Foo(String value) {
	}

}

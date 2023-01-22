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

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver.SchemaResolverCustomizer;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Integration tests for {@link PulsarBinderIntegrationTests}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@ExtendWith(OutputCaptureExtension.class)
class PulsarBinderIntegrationTests implements PulsarTestContainerSupport {

	@Nested
	class DefaultEncoding {

		@Test
		void primitiveTypeString(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveTextConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.cloud.function.definition=textSupplier;textLogger",
					"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
					"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.subscription-name=pbit-text-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(10))
						.until(() -> output.toString().contains("Hello binder: test-basic-scenario"));
			}
		}

		@Test
		void primitiveTypeFloat(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveFloatConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piSupplier-out-0.destination=pi-stream",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=pi-stream",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription-name=pbit-float-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(10))
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
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=piSupplier-out-0",
					"--spring.cloud.stream.bindings.piSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.piLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.schema-type=FLOAT",
					"--spring.cloud.stream.pulsar.bindings.piSupplier-out-0.producer.schema-type=FLOAT",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription-name=pbit-float-sub2")) {
				Awaitility.await().atMost(Duration.ofSeconds(10))
						.until(() -> output.toString().contains("Hello binder: 3.14"));
			}
		}

		@Test
		void jsonTypeFoo(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
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
				Awaitility.await().atMost(Duration.ofSeconds(10))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void jsonTypeFooWithNoSchemaTypeAndCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooWithCustomMappingConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub2",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(10))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void jsonTypeFooWithNoSchemaTypeAndNoCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-3",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-3",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub3",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName())) {
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(
						() -> output.toString().contains("Could not determine producer schema for foo-stream-3"));
			}
		}

		@Test
		void jsonTypeFooConsumerWithNoSchemaTypeAndNoCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConsumerConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-4",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-4",
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription-name=pbit-foo-sub4")) {
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(
						() -> output.toString().contains("Could not determine consumer schema for foo-stream-4"));
			}
		}

		@Test
		void keyValueTypeWithCustomFooMapping(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
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
				Awaitility.await().atMost(Duration.ofSeconds(10))
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

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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarBootstrapConfiguration;
import org.springframework.pulsar.annotation.PulsarListenerAnnotationBeanPostProcessor;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.CachingPulsarProducerFactory;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarReaderFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarClientBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.SchemaResolver.SchemaResolverCustomizer;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.function.PulsarFunctionAdministration;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.PulsarContainerProperties;

import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Autoconfiguration tests for {@link PulsarAutoConfiguration}.
 *
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Soby Chacko
 */
@SuppressWarnings("unchecked")
class PulsarAutoConfigurationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withConfiguration(AutoConfigurations.of(PulsarAutoConfiguration.class));

	@Test
	void autoConfigurationSkippedWhenPulsarTemplateNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(PulsarTemplate.class))
				.run((context) -> assertThat(context).hasNotFailed().doesNotHaveBean(PulsarAutoConfiguration.class));
	}

	@Test
	void annotationDrivenConfigurationSkippedWhenEnablePulsarAnnotationNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(EnablePulsar.class))
				.run((context) -> assertThat(context).hasNotFailed()
						.doesNotHaveBean(PulsarAnnotationDrivenConfiguration.class));
	}

	@Test
	void bootstrapConfigurationSkippedWhenCustomPulsarListenerAnnotationProcessorDefined() {
		this.contextRunner
				.withBean("org.springframework.pulsar.config.internalPulsarListenerAnnotationProcessor", String.class,
						() -> "someFauxBean")
				.run((context) -> assertThat(context).hasNotFailed()
						.doesNotHaveBean(PulsarBootstrapConfiguration.class));
	}

	@Test
	void defaultBeansAreAutoConfigured() {
		this.contextRunner.run((context) -> assertThat(context).hasSingleBean(PulsarClientBuilderConfigurer.class)
				.hasSingleBean(PulsarClient.class).hasSingleBean(PulsarProducerFactory.class)
				.hasSingleBean(PulsarTemplate.class).hasSingleBean(PulsarConsumerFactory.class)
				.hasSingleBean(ConcurrentPulsarListenerContainerFactory.class)
				.hasSingleBean(PulsarListenerAnnotationBeanPostProcessor.class)
				.hasSingleBean(PulsarListenerEndpointRegistry.class).hasSingleBean(PulsarAdministration.class)
				.hasSingleBean(DefaultSchemaResolver.class).hasSingleBean(DefaultTopicResolver.class));
	}

	@Nested
	class ProducerFactoryTests {

		@Test
		void customPulsarProducerFactoryIsRespected() {
			PulsarProducerFactory<String> producerFactory = mock(PulsarProducerFactory.class);
			contextRunner.withBean("customPulsarProducerFactory", PulsarProducerFactory.class, () -> producerFactory)
					.run((context) -> assertThat(context).hasNotFailed().getBean(PulsarProducerFactory.class)
							.isSameAs(producerFactory));
		}

		@Test
		void cachingProducerFactoryEnabledByDefault() {
			contextRunner.run((context) -> assertHasProducerFactoryOfType(CachingPulsarProducerFactory.class, context));
		}

		@Test
		void nonCachingProducerFactoryCanBeEnabled() {
			contextRunner.withPropertyValues("spring.pulsar.producer.cache.enabled=false")
					.run((context -> assertHasProducerFactoryOfType(DefaultPulsarProducerFactory.class, context)));
		}

		@Test
		void cachingProducerFactoryCanBeEnabled() {
			contextRunner.withPropertyValues("spring.pulsar.producer.cache.enabled=true")
					.run((context -> assertHasProducerFactoryOfType(CachingPulsarProducerFactory.class, context)));
		}

		@Test
		void cachingEnabledAndCaffeineNotOnClasspath() {
			contextRunner.withClassLoader(new FilteredClassLoader(Caffeine.class))
					.withPropertyValues("spring.pulsar.producer.cache.enabled=true")
					.run((context -> assertHasProducerFactoryOfType(CachingPulsarProducerFactory.class, context)));
		}

		@Test
		void cachingProducerFactoryCanBeConfigured() {
			contextRunner
					.withPropertyValues("spring.pulsar.producer.cache.expire-after-access=100s",
							"spring.pulsar.producer.cache.maximum-size=5150",
							"spring.pulsar.producer.cache.initial-capacity=200")
					.run((context -> assertThat(context).hasNotFailed().getBean(PulsarProducerFactory.class)
							.extracting("producerCache.cache.cache").hasFieldOrPropertyWithValue("maximum", 5150L)
							.hasFieldOrPropertyWithValue("expiresAfterAccessNanos", TimeUnit.SECONDS.toNanos(100))));
		}

		@Test
		void beansAreInjectedInNonCachingProducerFactory() {
			contextRunner.withUserConfiguration(SpyCustomizersConfig.class)
					.withPropertyValues("spring.pulsar.producer.topic-name=foo-topic",
							"spring.pulsar.producer.cache.enabled=false")
					.run((context) -> assertThat(context).getBean(DefaultPulsarProducerFactory.class)
							.hasFieldOrPropertyWithValue("defaultTopic", "foo-topic")
							.hasFieldOrPropertyWithValue("defaultConfigCustomizer",
									SpyCustomizersConfig.testProducerCustomizer)
							.hasFieldOrPropertyWithValue("pulsarClient", context.getBean(PulsarClient.class))
							.hasFieldOrPropertyWithValue("topicResolver", context.getBean(TopicResolver.class)));
		}

		@Test
		void beansAreInjectedInCachingProducerFactory() {
			contextRunner.withUserConfiguration(SpyCustomizersConfig.class)
					.withPropertyValues("spring.pulsar.producer.topic-name=foo-topic",
							"spring.pulsar.producer.cache.enabled=true")
					.run((context) -> assertThat(context).getBean(CachingPulsarProducerFactory.class)
							.hasFieldOrPropertyWithValue("defaultTopic", "foo-topic")
							.hasFieldOrPropertyWithValue("defaultConfigCustomizer",
									SpyCustomizersConfig.testProducerCustomizer)
							.hasFieldOrPropertyWithValue("pulsarClient", context.getBean(PulsarClient.class))
							.hasFieldOrPropertyWithValue("topicResolver", context.getBean(TopicResolver.class)));
		}

		private void assertHasProducerFactoryOfType(Class<?> producerFactoryType,
				AssertableApplicationContext context) {
			assertThat(context).hasNotFailed().hasSingleBean(PulsarProducerFactory.class)
					.getBean(PulsarProducerFactory.class).isExactlyInstanceOf(producerFactoryType);
		}

	}

	@Nested
	class ConsumerFactoryTests {

		@Test
		void customPulsarConsumerFactoryIsRespected() {
			PulsarConsumerFactory<String> consumerFactory = mock(PulsarConsumerFactory.class);
			contextRunner.withBean("customPulsarConsumerFactory", PulsarConsumerFactory.class, () -> consumerFactory)
					.run((context) -> assertThat(context).getBean(PulsarConsumerFactory.class)
							.isSameAs(consumerFactory));
		}

		@Test
		void beansAreInjectedInConsumerFactory() {
			contextRunner.withUserConfiguration(SpyCustomizersConfig.class)
					.run((context) -> assertThat(context).getBean(DefaultPulsarConsumerFactory.class)
							.hasFieldOrPropertyWithValue("pulsarClient", context.getBean(PulsarClient.class))
							.hasFieldOrPropertyWithValue("defaultConfigCustomizer",
									SpyCustomizersConfig.testConsumerCustomizer));
		}

	}

	/*
	 * Use '@TestConfiguration' and exact name of the PulsarProperties bean that is
	 * created via the '@EnableConfigurationProperties' on the actual auto-config in order
	 * to 'replace' the PulsarProperties bean - all of this effort is to make sure the
	 * returned producer/consumer builder customizer is the one we expect.
	 */
	@TestConfiguration(proxyBeanMethods = false)
	static class SpyCustomizersConfig {

		@SuppressWarnings("rawtypes")
		static ProducerBuilderCustomizer testProducerCustomizer = (producerBuilder) -> {
		};

		@SuppressWarnings("rawtypes")
		static ConsumerBuilderCustomizer testConsumerCustomizer = (consumerBuilder) -> {
		};

		@Bean(name = "spring.pulsar-org.springframework.pulsar.autoconfigure.PulsarProperties")
		PulsarProperties pulsarProperties() {
			var pulsarProps = new PulsarProperties();
			var producerProps = spy(pulsarProps.getProducer());
			when(producerProps.toProducerBuilderCustomizer()).thenReturn(testProducerCustomizer);
			var consumerProps = spy(pulsarProps.getConsumer());
			when(consumerProps.toConsumerBuilderCustomizer()).thenReturn(testConsumerCustomizer);

			var spyPulsarProps = spy(pulsarProps);
			when(spyPulsarProps.getProducer()).thenReturn(producerProps);
			when(spyPulsarProps.getConsumer()).thenReturn(consumerProps);
			return spyPulsarProps;
		}

	}

	@Nested
	class ReaderFactoryTests {

		@Test
		void readerFactoryIsAutoConfiguredByDefault() {
			contextRunner.run((context) -> assertThat(context).hasNotFailed().hasSingleBean(PulsarReaderFactory.class)
					.getBean(PulsarReaderFactory.class).isExactlyInstanceOf(DefaultPulsarReaderFactory.class));
		}

		@Test
		void readerFactoryCanBeConfigured() {
			contextRunner.withPropertyValues("spring.pulsar.reader.topic-names=foo",
					"spring.pulsar.reader.receiver-queue-size=200", "spring.pulsar.reader.reader-name=test-reader",
					"spring.pulsar.reader.subscription-name=test-subscription",
					"spring.pulsar.reader.subscription-role-prefix=test-prefix",
					"spring.pulsar.reader.read-compacted=true", "spring.pulsar.reader.reset-include-head=true")
					.run((context -> assertThat(context).hasNotFailed().getBean(PulsarReaderFactory.class)
							.extracting("readerConfig").hasFieldOrPropertyWithValue("topicNames", List.of("foo"))
							.hasFieldOrPropertyWithValue("receiverQueueSize", 200)
							.hasFieldOrPropertyWithValue("readerName", "test-reader")
							.hasFieldOrPropertyWithValue("subscriptionName", "test-subscription")
							.hasFieldOrPropertyWithValue("subscriptionRolePrefix", "test-prefix")
							.hasFieldOrPropertyWithValue("readCompacted", true)
							.hasFieldOrPropertyWithValue("resetIncludeHead", true)));
		}

	}

	@Nested
	class SchemaAndTopicResolversTests {

		@Test
		void customSchemaResolverIsRespected() {
			SchemaResolver customSchemaResolver = mock(SchemaResolver.class);
			contextRunner.withBean("customSchemaResolver", SchemaResolver.class, () -> customSchemaResolver)
					.run((context) -> assertThat(context).hasNotFailed().getBean(SchemaResolver.class)
							.isSameAs(customSchemaResolver));
		}

		@Test
		void defaultSchemaResolverCanBeCustomized() {
			record Foo() {
			}
			SchemaResolverCustomizer<DefaultSchemaResolver> customizer = (sr) -> sr.addCustomSchemaMapping(Foo.class,
					Schema.STRING);
			contextRunner.withBean("schemaResolverCustomizer", SchemaResolverCustomizer.class, () -> customizer)
					.run((context) -> assertThat(context).hasNotFailed().getBean(DefaultSchemaResolver.class)
							.extracting(DefaultSchemaResolver::getCustomSchemaMappings, InstanceOfAssertFactories.MAP)
							.containsEntry(Foo.class, Schema.STRING));
		}

		@Test
		void customTopicResolverIsRespected() {
			TopicResolver customTopicResolver = mock(TopicResolver.class);
			contextRunner.withBean("customTopicResolver", TopicResolver.class, () -> customTopicResolver)
					.run((context) -> assertThat(context).hasNotFailed().getBean(TopicResolver.class)
							.isSameAs(customTopicResolver));
		}

	}

	@Nested
	class PulsarTemplateTests {

		@Test
		void customPulsarTemplateIsRespected() {
			PulsarTemplate<String> template = mock(PulsarTemplate.class);
			contextRunner.withBean("customPulsarTemplate", PulsarTemplate.class, () -> template).run(
					(context) -> assertThat(context).hasNotFailed().getBean(PulsarTemplate.class).isSameAs(template));
		}

		@Test
		void beansAreInjectedInPulsarTemplate() {
			PulsarProducerFactory<?> producerFactory = mock(PulsarProducerFactory.class);
			SchemaResolver schemaResolver = mock(SchemaResolver.class);
			TopicResolver topicResolver = mock(TopicResolver.class);
			contextRunner.withBean("customPulsarProducerFactory", PulsarProducerFactory.class, () -> producerFactory)
					.withBean("schemaResolver", SchemaResolver.class, () -> schemaResolver)
					.withBean("topicResolver", TopicResolver.class, () -> topicResolver)
					.run((context -> assertThat(context).hasNotFailed().getBean(PulsarTemplate.class)
							.hasFieldOrPropertyWithValue("producerFactory", producerFactory)
							.hasFieldOrPropertyWithValue("schemaResolver", schemaResolver)
							.hasFieldOrPropertyWithValue("topicResolver", topicResolver)));
		}

		@Test
		void customProducerInterceptorIsUsedInPulsarTemplate() {
			ProducerInterceptor interceptor = mock(ProducerInterceptor.class);
			contextRunner.withBean("customProducerInterceptor", ProducerInterceptor.class, () -> interceptor)
					.run((context -> assertThat(context).hasNotFailed().getBean(PulsarTemplate.class)
							.extracting("interceptors")
							.asInstanceOf(InstanceOfAssertFactories.list(ProducerInterceptor.class))
							.contains(interceptor)));
		}

		@Test
		void customProducerInterceptorsOrderedProperly() {
			contextRunner.withUserConfiguration(InterceptorTestConfiguration.class)
					.run((context -> assertThat(context).hasNotFailed().getBean(PulsarTemplate.class)
							.extracting("interceptors")
							.asInstanceOf(InstanceOfAssertFactories.list(ProducerInterceptor.class))
							.containsExactly(InterceptorTestConfiguration.interceptorBar,
									InterceptorTestConfiguration.interceptorFoo)));
		}

		@Configuration(proxyBeanMethods = false)
		static class InterceptorTestConfiguration {

			static ProducerInterceptor interceptorFoo = mock(ProducerInterceptor.class);
			static ProducerInterceptor interceptorBar = mock(ProducerInterceptor.class);

			@Bean
			@Order(200)
			ProducerInterceptor interceptorFoo() {
				return interceptorFoo;
			}

			@Bean
			@Order(100)
			ProducerInterceptor interceptorBar() {
				return interceptorBar;
			}

		}

	}

	@Nested
	class PulsarListenerTests {

		@Test
		void customPulsarListenerContainerFactoryIsRespected() {
			PulsarListenerContainerFactory listenerContainerFactory = mock(PulsarListenerContainerFactory.class);
			contextRunner
					.withBean("pulsarListenerContainerFactory", PulsarListenerContainerFactory.class,
							() -> listenerContainerFactory)
					.run((context) -> assertThat(context).hasNotFailed().getBean(PulsarListenerContainerFactory.class)
							.isSameAs(listenerContainerFactory));
		}

		@Test
		void beansAreInjectedInPulsarListenerContainerFactory() {
			PulsarConsumerFactory<?> consumerFactory = mock(PulsarConsumerFactory.class);
			SchemaResolver schemaResolver = mock(SchemaResolver.class);
			TopicResolver topicResolver = mock(TopicResolver.class);
			contextRunner.withBean("pulsarConsumerFactory", PulsarConsumerFactory.class, () -> consumerFactory)
					.withBean("schemaResolver", SchemaResolver.class, () -> schemaResolver)
					.withBean("topicResolver", TopicResolver.class, () -> topicResolver)
					.run((context -> assertThat(context).hasNotFailed()
							.getBean(ConcurrentPulsarListenerContainerFactory.class)
							.hasFieldOrPropertyWithValue("consumerFactory", consumerFactory)
							.extracting(ConcurrentPulsarListenerContainerFactory<Object>::getContainerProperties)
							.hasFieldOrPropertyWithValue("schemaResolver", schemaResolver)
							.hasFieldOrPropertyWithValue("topicResolver", topicResolver)));
		}

		@Test
		void customPulsarListenerAnnotationBeanPostProcessorIsRespected() {
			PulsarListenerAnnotationBeanPostProcessor<String> listenerAnnotationBeanPostProcessor = mock(
					PulsarListenerAnnotationBeanPostProcessor.class);
			contextRunner
					.withBean("org.springframework.pulsar.config.internalPulsarListenerAnnotationProcessor",
							PulsarListenerAnnotationBeanPostProcessor.class, () -> listenerAnnotationBeanPostProcessor)
					.run((context) -> assertThat(context).hasNotFailed()
							.getBean(PulsarListenerAnnotationBeanPostProcessor.class)
							.isSameAs(listenerAnnotationBeanPostProcessor));
		}

		@Test
		void listenerPropertiesAreHonored() {
			contextRunner.withPropertyValues("spring.pulsar.listener.ack-mode=manual",
					"spring.pulsar.listener.schema-type=avro", "spring.pulsar.listener.max-num-messages=10",
					"spring.pulsar.listener.max-num-bytes=101B", "spring.pulsar.listener.batch-timeout=50ms",
					"spring.pulsar.consumer.subscription-type=shared").run((context -> {
						AbstractObjectAssert<?, PulsarContainerProperties> properties = assertThat(context)
								.hasNotFailed().getBean(ConcurrentPulsarListenerContainerFactory.class)
								.extracting(ConcurrentPulsarListenerContainerFactory<Object>::getContainerProperties);
						properties.extracting(PulsarContainerProperties::getAckMode).isEqualTo(AckMode.MANUAL);
						properties.extracting(PulsarContainerProperties::getSchemaType).isEqualTo(SchemaType.AVRO);
						properties.extracting(PulsarContainerProperties::getMaxNumMessages).isEqualTo(10);
						properties.extracting(PulsarContainerProperties::getMaxNumBytes).isEqualTo(101);
						properties.extracting(PulsarContainerProperties::getBatchTimeoutMillis).isEqualTo(50);
						properties.extracting(PulsarContainerProperties::getSubscriptionType)
								.isEqualTo(SubscriptionType.Shared);
					}));
		}

	}

	@Nested
	class PulsarClientTests {

		@Test
		void customPulsarClientIsRespected() {
			var customClient = mock(PulsarClient.class);
			contextRunner.withBean("customPulsarClient", PulsarClient.class, () -> customClient)
					.run((context) -> assertThat(context).getBean(PulsarClient.class).isSameAs(customClient));
		}

		@Test
		void customPulsarClientBuilderConfigurerIsRespected() {
			var customConfigurer = new PulsarClientBuilderConfigurer(new PulsarProperties(), Collections.emptyList());
			contextRunner
					.withBean("customPulsarClientConfigurer", PulsarClientBuilderConfigurer.class,
							() -> customConfigurer)
					.run((context) -> assertThat(context).getBean(PulsarClientBuilderConfigurer.class)
							.isSameAs(customConfigurer));
		}

		@Test
		void clientConfigurerWithNoUserDefinedCustomizers() {
			contextRunner.run((context) -> assertThat(context).getBean(PulsarClientBuilderConfigurer.class)
					.hasFieldOrPropertyWithValue("customizers", Collections.emptyList()));
		}

		@Test
		void clientConfigurerWithUserDefinedCustomizers() {
			contextRunner.withUserConfiguration(ClientCustomizersTestConfiguration.class)
					.run((context) -> assertThat(context).getBean(PulsarClientBuilderConfigurer.class)
							.extracting("customizers", InstanceOfAssertFactories.LIST)
							.containsExactly(ClientCustomizersTestConfiguration.clientCustomizerBar,
									ClientCustomizersTestConfiguration.clientCustomizerFoo));
		}

		@Test
		void clientConfigurerIsApplied() {
			var clientConfigurer = spy(
					new PulsarClientBuilderConfigurer(new PulsarProperties(), Collections.emptyList()));
			contextRunner.withBean("clientConfigurer", PulsarClientBuilderConfigurer.class, () -> clientConfigurer)
					.run((context) -> verify(clientConfigurer).configure(any(ClientBuilder.class)));
		}

		@Configuration(proxyBeanMethods = false)
		static class ClientCustomizersTestConfiguration {

			static PulsarClientBuilderCustomizer clientCustomizerFoo = mock(PulsarClientBuilderCustomizer.class);
			static PulsarClientBuilderCustomizer clientCustomizerBar = mock(PulsarClientBuilderCustomizer.class);

			@Bean
			@Order(200)
			PulsarClientBuilderCustomizer clientCustomizerFoo() {
				return clientCustomizerFoo;
			}

			@Bean
			@Order(100)
			PulsarClientBuilderCustomizer clientCustomizerBar() {
				return clientCustomizerBar;
			}

		}

	}

	@Nested
	class PulsarAdministrationTests {

		@Test
		void customPulsarAdministrationIsRespected() {
			PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
			contextRunner.withBean("customPulsarAdministration", PulsarAdministration.class, () -> pulsarAdministration)
					.run((context) -> assertThat(context).hasNotFailed().getBean(PulsarAdministration.class)
							.isSameAs(pulsarAdministration));
		}

	}

	@Nested
	class DefaultsTypeMappingsTests {

		@Test
		void topicMappingsAreAddedToTopicResolver() {
			contextRunner
					.withPropertyValues(
							"spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(Foo.class.getName()),
							"spring.pulsar.defaults.type-mappings[0].topic-name=foo-topic",
							"spring.pulsar.defaults.type-mappings[1].message-type=%s".formatted(String.class.getName()),
							"spring.pulsar.defaults.type-mappings[1].topic-name=string-topic")
					.run((context -> assertThat(context).hasNotFailed().getBean(TopicResolver.class)
							.asInstanceOf(InstanceOfAssertFactories.type(DefaultTopicResolver.class))
							.extracting(DefaultTopicResolver::getCustomTopicMappings, InstanceOfAssertFactories.MAP)
							.containsOnly(entry(Foo.class, "foo-topic"), entry(String.class, "string-topic"))));
		}

		@Test
		void schemaMappingForPrimitiveIsAddedToSchemaResolver() {
			contextRunner
					.withPropertyValues(
							"spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(Foo.class.getName()),
							"spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=STRING")
					.run((context -> assertThat(context).hasNotFailed().getBean(SchemaResolver.class)
							.asInstanceOf(InstanceOfAssertFactories.type(DefaultSchemaResolver.class))
							.extracting(DefaultSchemaResolver::getCustomSchemaMappings, InstanceOfAssertFactories.MAP)
							.containsOnly(entry(Foo.class, Schema.STRING))));
		}

		@Test
		void schemaMappingForStructIsAddedToSchemaResolver() {
			contextRunner
					.withPropertyValues(
							"spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(Foo.class.getName()),
							"spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=JSON")
					.run((context -> assertThat(context).hasNotFailed().getBean(SchemaResolver.class)
							.asInstanceOf(InstanceOfAssertFactories.type(DefaultSchemaResolver.class))
							.extracting(DefaultSchemaResolver::getCustomSchemaMappings,
									InstanceOfAssertFactories.map(Class.class, Schema.class))
							.hasEntrySatisfying(Foo.class,
									(schema) -> assertSchemaEquals(schema, Schema.JSON(Foo.class)))));
		}

		@Test
		void schemaMappingForKeyValueIsAddedToSchemaResolver() {
			contextRunner
					.withPropertyValues(
							"spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(Foo.class.getName()),
							"spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=%s"
									.formatted(SchemaType.KEY_VALUE.name()),
							"spring.pulsar.defaults.type-mappings[0].schema-info.message-key-type=%s"
									.formatted(String.class.getName()))
					.run((context -> assertThat(context).hasNotFailed().getBean(SchemaResolver.class)
							.asInstanceOf(InstanceOfAssertFactories.type(DefaultSchemaResolver.class))
							.extracting(DefaultSchemaResolver::getCustomSchemaMappings,
									InstanceOfAssertFactories.map(Class.class, Schema.class))
							.hasEntrySatisfying(Foo.class, (schema) -> assertSchemaEquals(schema, Schema
									.KeyValue(Schema.STRING, Schema.JSON(Foo.class), KeyValueEncodingType.INLINE)))));
		}

		private void assertSchemaEquals(Schema<?> left, Schema<?> right) {
			assertThat(left.getSchemaInfo()).isEqualTo(right.getSchemaInfo());
		}

		record Foo() {
		}

	}

	@Nested
	class FunctionTests {

		@Test
		void functionSupportEnabledByDefault() {
			// NOTE: hasNoNullFieldsOrProperties() ensures object providers set
			contextRunner.run(context -> assertThat(context).hasNotFailed().getBean(PulsarFunctionAdministration.class)
					.hasFieldOrPropertyWithValue("failFast", Boolean.TRUE)
					.hasFieldOrPropertyWithValue("propagateFailures", Boolean.TRUE)
					.hasFieldOrPropertyWithValue("propagateStopFailures", Boolean.FALSE).hasNoNullFieldsOrProperties()
					.extracting("pulsarAdministration").isSameAs(context.getBean(PulsarAdministration.class)));
		}

		@Test
		void functionSupportCanBeConfigured() {
			contextRunner
					.withPropertyValues("spring.pulsar.function.fail-fast=false",
							"spring.pulsar.function.propagate-failures=false",
							"spring.pulsar.function.propagate-stop-failures=true")
					.run(context -> assertThat(context).hasNotFailed().getBean(PulsarFunctionAdministration.class)
							.hasFieldOrPropertyWithValue("failFast", Boolean.FALSE)
							.hasFieldOrPropertyWithValue("propagateFailures", Boolean.FALSE)
							.hasFieldOrPropertyWithValue("propagateStopFailures", Boolean.TRUE));
		}

		@Test
		void functionSupportCanBeDisabled() {
			contextRunner.withPropertyValues("spring.pulsar.function.enabled=false").run(
					context -> assertThat(context).hasNotFailed().doesNotHaveBean(PulsarFunctionAdministration.class));
		}

		@Test
		void customFunctionAdminIsRespected() {
			PulsarFunctionAdministration customFunctionAdmin = mock(PulsarFunctionAdministration.class);
			contextRunner.withBean(PulsarFunctionAdministration.class, () -> customFunctionAdmin)
					.run(context -> assertThat(context).hasNotFailed().getBean(PulsarFunctionAdministration.class)
							.isSameAs(customFunctionAdmin));
		}

	}

	@Nested
	class ObservationTests {

		@Test
		void templateObservationsEnabledByDefault() {
			contextRunner.run((context -> assertThat(context).getBean(PulsarTemplate.class)
					.hasFieldOrPropertyWithValue("observationEnabled", true)));
		}

		@Test
		void templateObservationsEnabledExplicitly() {
			contextRunner.withPropertyValues("spring.pulsar.template.observations-enabled=true")
					.run((context -> assertThat(context).getBean(PulsarTemplate.class)
							.hasFieldOrPropertyWithValue("observationEnabled", true)));
		}

		@Test
		void templateObservationsCanBeDisabled() {
			contextRunner.withPropertyValues("spring.pulsar.template.observations-enabled=false")
					.run((context -> assertThat(context).getBean(PulsarTemplate.class)
							.hasFieldOrPropertyWithValue("observationEnabled", false)));
		}

		@Test
		void listenerObservationsEnabledByDefault() {
			contextRunner.run((context -> assertThat(context).getBean(ConcurrentPulsarListenerContainerFactory.class)
					.hasFieldOrPropertyWithValue("containerProperties.observationEnabled", true)));
		}

		@Test
		void listenerObservationsEnabledExplicitly() {
			contextRunner.withPropertyValues("spring.pulsar.listener.observations-enabled=true")
					.run((context -> assertThat(context).getBean(ConcurrentPulsarListenerContainerFactory.class)
							.hasFieldOrPropertyWithValue("containerProperties.observationEnabled", true)));
		}

		@Test
		void listenerObservationsCanBeDisabled() {
			contextRunner.withPropertyValues("spring.pulsar.listener.observations-enabled=false")
					.run((context -> assertThat(context).getBean(ConcurrentPulsarListenerContainerFactory.class)
							.hasFieldOrPropertyWithValue("containerProperties.observationEnabled", false)));
		}

	}

}

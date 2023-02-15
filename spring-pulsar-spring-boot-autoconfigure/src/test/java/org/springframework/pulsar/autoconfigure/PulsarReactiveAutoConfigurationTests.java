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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.adapter.ProducerCacheProvider;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.apache.pulsar.reactive.client.producercache.CaffeineProducerCacheProvider;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.EnableReactivePulsar;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarBootstrapConfiguration;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerAnnotationBeanPostProcessor;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarReaderFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarReaderFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;

/**
 * Autoconfiguration tests for {@link PulsarReactiveAutoConfiguration}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
@SuppressWarnings("unchecked")
class PulsarReactiveAutoConfigurationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withConfiguration(AutoConfigurations.of(PulsarAutoConfiguration.class))
			.withConfiguration(AutoConfigurations.of(PulsarReactiveAutoConfiguration.class));

	@Test
	void autoConfigurationSkippedWhenReactivePulsarClientNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(ReactivePulsarClient.class)).run(
				(context) -> assertThat(context).hasNotFailed().doesNotHaveBean(PulsarReactiveAutoConfiguration.class));
	}

	@Test
	void autoConfigurationSkippedWhenReactivePulsarTemplateNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(ReactivePulsarTemplate.class)).run(
				(context) -> assertThat(context).hasNotFailed().doesNotHaveBean(PulsarReactiveAutoConfiguration.class));
	}

	@Test
	void annotationDrivenConfigurationSkippedWhenEnablePulsarAnnotationNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(EnableReactivePulsar.class))
				.run((context) -> assertThat(context).hasNotFailed()
						.doesNotHaveBean(PulsarReactiveAnnotationDrivenConfiguration.class));
	}

	@Test
	void bootstrapConfigurationSkippedWhenCustomReactivePulsarListenerAnnotationProcessorDefined() {
		this.contextRunner
				.withBean("org.springframework.pulsar.config.internalReactivePulsarListenerAnnotationProcessor",
						String.class, () -> "someFauxBean")
				.run((context) -> assertThat(context).hasNotFailed()
						.doesNotHaveBean(ReactivePulsarBootstrapConfiguration.class));
	}

	@Test
	void defaultBeansAreAutoConfigured() {
		this.contextRunner.run((context) -> assertThat(context).hasNotFailed()
				.hasSingleBean(ReactivePulsarTemplate.class).hasSingleBean(ReactivePulsarClient.class)
				.hasSingleBean(ProducerCacheProvider.class).hasSingleBean(ReactiveMessageSenderCache.class)
				.hasSingleBean(ReactivePulsarSenderFactory.class).hasSingleBean(ReactivePulsarTemplate.class)
				.hasSingleBean(DefaultReactivePulsarListenerContainerFactory.class)
				.hasSingleBean(ReactivePulsarListenerAnnotationBeanPostProcessor.class)
				.hasSingleBean(ReactivePulsarListenerEndpointRegistry.class));
	}

	@ParameterizedTest
	@ValueSource(classes = { ReactivePulsarClient.class, ProducerCacheProvider.class, ReactiveMessageSenderCache.class,
			ReactivePulsarSenderFactory.class, ReactivePulsarConsumerFactory.class, ReactivePulsarReaderFactory.class,
			ReactivePulsarTemplate.class })
	<T> void customBeanIsRespected(Class<T> beanClass) {
		T bean = mock(beanClass);
		this.contextRunner.withBean(beanClass.getName(), beanClass, () -> bean)
				.run((context) -> assertThat(context).hasNotFailed().getBean(beanClass).isSameAs(bean));
	}

	@SuppressWarnings("rawtypes")
	@Test
	void beansAreInjectedInReactivePulsarListenerContainerFactory() {
		ReactivePulsarConsumerFactory<?> consumerFactory = mock(ReactivePulsarConsumerFactory.class);
		SchemaResolver schemaResolver = mock(SchemaResolver.class);
		this.contextRunner
				.withBean("customReactivePulsarConsumerFactory", ReactivePulsarConsumerFactory.class,
						() -> consumerFactory)
				.withBean("schemaResolver", SchemaResolver.class, () -> schemaResolver).run((context -> {
					AbstractObjectAssert<? extends AbstractObjectAssert<?, DefaultReactivePulsarListenerContainerFactory>, DefaultReactivePulsarListenerContainerFactory> containerFactory = assertThat(
							context).hasNotFailed().getBean(DefaultReactivePulsarListenerContainerFactory.class);
					containerFactory.extracting("consumerFactory").isSameAs(consumerFactory);
					containerFactory.extracting(DefaultReactivePulsarListenerContainerFactory::getContainerProperties)
							.extracting(ReactivePulsarContainerProperties::getSchemaResolver).isSameAs(schemaResolver);
				}));
	}

	@Test
	void customReactivePulsarListenerContainerFactoryIsRespected() {
		ReactivePulsarListenerContainerFactory<String> listenerContainerFactory = mock(
				ReactivePulsarListenerContainerFactory.class);
		this.contextRunner
				.withBean("reactivePulsarListenerContainerFactory", ReactivePulsarListenerContainerFactory.class,
						() -> listenerContainerFactory)
				.run((context) -> assertThat(context).hasNotFailed()
						.getBean(ReactivePulsarListenerContainerFactory.class).isSameAs(listenerContainerFactory));
	}

	@Test
	void customReactivePulsarListenerAnnotationBeanPostProcessorIsRespected() {
		ReactivePulsarListenerAnnotationBeanPostProcessor<String> listenerAnnotationBeanPostProcessor = mock(
				ReactivePulsarListenerAnnotationBeanPostProcessor.class);
		this.contextRunner
				.withBean("org.springframework.pulsar.config.internalReactivePulsarListenerAnnotationProcessor",
						ReactivePulsarListenerAnnotationBeanPostProcessor.class,
						() -> listenerAnnotationBeanPostProcessor)
				.run((context) -> assertThat(context).hasNotFailed()
						.getBean(ReactivePulsarListenerAnnotationBeanPostProcessor.class)
						.isSameAs(listenerAnnotationBeanPostProcessor));
	}

	@Test
	@SuppressWarnings("rawtypes")
	void beansAreInjectedInReactivePulsarTemplate() {
		ReactivePulsarSenderFactory<?> senderFactory = mock(ReactivePulsarSenderFactory.class);
		SchemaResolver schemaResolver = mock(SchemaResolver.class);
		this.contextRunner
				.withBean("customReactivePulsarSenderFactory", ReactivePulsarSenderFactory.class, () -> senderFactory)
				.withBean("schemaResolver", SchemaResolver.class, () -> schemaResolver).run((context -> {
					AbstractObjectAssert<? extends AbstractObjectAssert<?, ReactivePulsarTemplate>, ReactivePulsarTemplate> template = assertThat(
							context).hasNotFailed().getBean(ReactivePulsarTemplate.class);
					template.extracting("reactiveMessageSenderFactory").isSameAs(senderFactory);
					template.extracting("schemaResolver").isSameAs(schemaResolver);
				}));
	}

	@Test
	@SuppressWarnings("rawtypes")
	void beansAreInjectedInReactivePulsarSenderFactory() throws Exception {
		ReactivePulsarClient client = mock(ReactivePulsarClient.class);
		try (ReactiveMessageSenderCache cache = mock(ReactiveMessageSenderCache.class)) {
			this.contextRunner.withPropertyValues("spring.pulsar.reactive.sender.topic-name=test-topic")
					.withBean("customReactivePulsarClient", ReactivePulsarClient.class, () -> client)
					.withBean("customReactiveMessageSenderCache", ReactiveMessageSenderCache.class, () -> cache)
					.run((context -> {
						AbstractObjectAssert<? extends AbstractObjectAssert<?, DefaultReactivePulsarSenderFactory>, DefaultReactivePulsarSenderFactory> senderFactory = assertThat(
								context).hasNotFailed().getBean(DefaultReactivePulsarSenderFactory.class);
						senderFactory.extracting(DefaultReactivePulsarSenderFactory::getReactiveMessageSenderSpec)
								.extracting(ReactiveMessageSenderSpec::getTopicName).isEqualTo("test-topic");
						senderFactory.extracting("reactivePulsarClient",
								InstanceOfAssertFactories.type(ReactivePulsarClient.class)).isSameAs(client);
						senderFactory
								.extracting("reactiveMessageSenderCache",
										InstanceOfAssertFactories.type(ReactiveMessageSenderCache.class))
								.isSameAs(cache);
						senderFactory.extracting("topicResolver", InstanceOfAssertFactories.type(TopicResolver.class))
								.isSameAs(context.getBean(TopicResolver.class));

					}));
		}
	}

	@Test
	@SuppressWarnings("rawtypes")
	void beansAreInjectedInReactivePulsarConsumerFactory() {
		ReactivePulsarClient client = mock(ReactivePulsarClient.class);
		this.contextRunner.withPropertyValues("spring.pulsar.reactive.consumer.consumer-name=test-consumer")
				.withBean("customReactivePulsarClient", ReactivePulsarClient.class, () -> client).run((context -> {
					AbstractObjectAssert<? extends AbstractObjectAssert<?, DefaultReactivePulsarConsumerFactory>, DefaultReactivePulsarConsumerFactory> senderFactory = assertThat(
							context).hasNotFailed().getBean(DefaultReactivePulsarConsumerFactory.class);
					senderFactory
							.extracting("consumerSpec",
									InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
							.extracting(ReactiveMessageConsumerSpec::getConsumerName).isEqualTo("test-consumer");
					senderFactory.extracting("reactivePulsarClient",
							InstanceOfAssertFactories.type(ReactivePulsarClient.class)).isSameAs(client);
				}));

	}

	@Test
	@SuppressWarnings("rawtypes")
	void beansAreInjectedInReactivePulsarReaderFactory() {
		ReactivePulsarClient client = mock(ReactivePulsarClient.class);
		this.contextRunner.withPropertyValues("spring.pulsar.reactive.reader.reader-name=test-reader")
				.withBean("customReactivePulsarClient", ReactivePulsarClient.class, () -> client).run((context -> {
					AbstractObjectAssert<? extends AbstractObjectAssert<?, DefaultReactivePulsarReaderFactory>, DefaultReactivePulsarReaderFactory> senderFactory = assertThat(
							context).hasNotFailed().getBean(DefaultReactivePulsarReaderFactory.class);
					senderFactory
							.extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
							.extracting(ReactiveMessageReaderSpec::getReaderName).isEqualTo("test-reader");
					senderFactory.extracting("reactivePulsarClient",
							InstanceOfAssertFactories.type(ReactivePulsarClient.class)).isSameAs(client);
				}));
	}

	@Test
	void beansAreInjectedInReactiveMessageSenderCache() throws Exception {
		try (ProducerCacheProvider provider = mock(ProducerCacheProvider.class)) {
			this.contextRunner.withBean("customProducerCacheProvider", ProducerCacheProvider.class, () -> provider)
					.run((context -> {
						var senderFactory = assertThat(context).hasNotFailed()
								.getBean(ReactiveMessageSenderCache.class);
						senderFactory.extracting("cacheProvider")
								.asInstanceOf(InstanceOfAssertFactories.type(ProducerCacheProvider.class))
								.isSameAs(provider);
					}));
		}
	}

	@Test
	@SuppressWarnings("rawtypes")
	void beansAreInjectedInReactivePulsarClient() throws Exception {
		try (PulsarClient client = mock(PulsarClient.class)) {
			PulsarClientFactoryBean factoryBean = new PulsarClientFactoryBean(Collections.emptyMap()) {
				@Override
				protected PulsarClient createInstance() {
					return client;
				}
			};
			this.contextRunner.withBean("customPulsarClient", PulsarClientFactoryBean.class, () -> factoryBean)
					.run((context -> assertThat(context).hasNotFailed().getBean(ReactivePulsarClient.class)
							.extracting("reactivePulsarResourceAdapter")
							.extracting("pulsarClientSupplier", InstanceOfAssertFactories.type(Supplier.class))
							.extracting(Supplier::get).isSameAs(client)));
		}
	}

	@Test
	void reactiveListenerPropertiesAreHonored() {
		contextRunner.withPropertyValues("spring.pulsar.reactive.listener.schema-type=avro",
				"spring.pulsar.reactive.listener.handling-timeout=10s",
				"spring.pulsar.reactive.listener.use-key-ordered-processing=true",
				"spring.pulsar.reactive.consumer.subscription-type=shared").run((context -> {
					AbstractObjectAssert<?, ReactivePulsarContainerProperties<?>> properties = assertThat(context)
							.hasNotFailed().getBean(DefaultReactivePulsarListenerContainerFactory.class)
							.extracting(DefaultReactivePulsarListenerContainerFactory<Object>::getContainerProperties);
					properties.extracting(ReactivePulsarContainerProperties::getSchemaType).isEqualTo(SchemaType.AVRO);
					properties.extracting(ReactivePulsarContainerProperties::getHandlingTimeout)
							.isEqualTo(Duration.ofSeconds(10));
					properties.extracting(ReactivePulsarContainerProperties::isUseKeyOrderedProcessing).isEqualTo(true);
					properties.extracting(ReactivePulsarContainerProperties::getSubscriptionType)
							.isEqualTo(SubscriptionType.Shared);
				}));
	}

	@Nested
	class SenderCacheAutoConfigurationTests {

		@Test
		void caffeineCacheUsedByDefault() {
			contextRunner.run(this::assertCaffeineProducerCacheProvider);
		}

		@Test
		void caffeineCacheCanBeConfigured() {
			contextRunner
					.withPropertyValues("spring.pulsar.reactive.sender.cache.expire-after-access=100s",
							"spring.pulsar.reactive.sender.cache.maximum-size=5150",
							"spring.pulsar.reactive.sender.cache.initial-capacity=200")
					.run((context) -> assertCaffeineProducerCacheProvider(context).extracting("cache")
							.extracting("cache").hasFieldOrPropertyWithValue("maximum", 5150L)
							.hasFieldOrPropertyWithValue("expiresAfterAccessNanos", TimeUnit.SECONDS.toNanos(100)));
		}

		@Test
		void defaultClientCacheIsUsedIfCaffeineProducerCacheProviderNotOnClasspath() {
			ReactiveMessageSenderCache cache = AdaptedReactivePulsarClientFactory.createCache();
			try (MockedStatic<AdaptedReactivePulsarClientFactory> mockedClientFactory = Mockito
					.mockStatic(AdaptedReactivePulsarClientFactory.class)) {
				mockedClientFactory.when(AdaptedReactivePulsarClientFactory::createCache).thenReturn(cache);
				mockedClientFactory.when(() -> AdaptedReactivePulsarClientFactory.create(any(PulsarClient.class)))
						.thenReturn(mock(ReactivePulsarClient.class));
				contextRunner.withClassLoader(new FilteredClassLoader(CaffeineProducerCacheProvider.class))
						.run((context) -> assertThat(context).hasNotFailed()
								.doesNotHaveBean(ProducerCacheProvider.class)
								.hasSingleBean(ReactiveMessageSenderCache.class)
								.getBean(ReactiveMessageSenderCache.class).isSameAs(cache));
				mockedClientFactory.verify(AdaptedReactivePulsarClientFactory::createCache);
			}
		}

		@Test
		void cacheCanBeDisabled() {
			contextRunner.withPropertyValues("spring.pulsar.reactive.sender.cache.enabled=false")
					.run((context -> assertThat(context).hasNotFailed().doesNotHaveBean(ProducerCacheProvider.class)
							.doesNotHaveBean(ReactiveMessageSenderCache.class)));
		}

		private AbstractObjectAssert<?, ProducerCacheProvider> assertCaffeineProducerCacheProvider(
				AssertableApplicationContext context) {
			return assertThat(context).hasNotFailed().hasSingleBean(ProducerCacheProvider.class)
					.hasSingleBean(ReactiveMessageSenderCache.class).getBean(ProducerCacheProvider.class)
					.isExactlyInstanceOf(CaffeineProducerCacheProvider.class);
		}

	}

}

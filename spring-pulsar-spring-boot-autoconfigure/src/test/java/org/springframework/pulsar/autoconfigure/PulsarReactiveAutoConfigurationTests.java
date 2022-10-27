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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.adapter.ProducerCacheProvider;
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
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderTemplate;

/**
 * Autoconfiguration tests for {@link PulsarReactiveAutoConfiguration}.
 *
 * @author Christophe Bornet
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
	void autoConfigurationSkippedWhenReactivePulsarSenderTemplateNotOnClasspath() {
		this.contextRunner.withClassLoader(new FilteredClassLoader(ReactivePulsarSenderTemplate.class)).run(
				(context) -> assertThat(context).hasNotFailed().doesNotHaveBean(PulsarReactiveAutoConfiguration.class));
	}

	@Test
	void defaultBeansAreAutoConfigured() {
		this.contextRunner.run((context) -> assertThat(context).hasNotFailed()
				.hasSingleBean(ReactivePulsarSenderTemplate.class).hasSingleBean(ReactivePulsarClient.class)
				.hasSingleBean(ProducerCacheProvider.class).hasSingleBean(ReactiveMessageSenderCache.class)
				.hasSingleBean(ReactivePulsarSenderFactory.class).getBean(ReactivePulsarSenderTemplate.class));
	}

	@ParameterizedTest
	@ValueSource(classes = { ReactivePulsarClient.class, ProducerCacheProvider.class, ReactiveMessageSenderCache.class,
			ReactivePulsarSenderFactory.class, ReactivePulsarSenderTemplate.class })
	<T> void customBeanIsRespected(Class<T> beanClass) {
		T bean = mock(beanClass);
		this.contextRunner.withBean(beanClass.getName(), beanClass, () -> bean)
				.run((context) -> assertThat(context).hasNotFailed().getBean(beanClass).isSameAs(bean));
	}

	@Test
	void beansAreInjectedInReactivePulsarTemplate() {
		ReactivePulsarSenderFactory<?> senderFactory = mock(ReactivePulsarSenderFactory.class);
		this.contextRunner
				.withBean("customReactivePulsarSenderFactory", ReactivePulsarSenderFactory.class, () -> senderFactory)
				.run((context -> assertThat(context).hasNotFailed().getBean(ReactivePulsarSenderTemplate.class)
						.extracting("reactiveMessageSenderFactory")
						.asInstanceOf(InstanceOfAssertFactories.type(ReactivePulsarSenderFactory.class))
						.isSameAs(senderFactory)));
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
					}));
		}
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
			PulsarClientFactoryBean factoryBean = new PulsarClientFactoryBean(null) {
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
			contextRunner.withPropertyValues("spring.pulsar.reactive.sender.cache.enabled=false").run((context -> {
				assertThat(context).hasNotFailed().doesNotHaveBean(ProducerCacheProvider.class)
						.doesNotHaveBean(ReactiveMessageSenderCache.class);
			}));
		}

		private AbstractObjectAssert<?, ProducerCacheProvider> assertCaffeineProducerCacheProvider(
				AssertableApplicationContext context) {
			return assertThat(context).hasNotFailed().hasSingleBean(ProducerCacheProvider.class)
					.hasSingleBean(ReactiveMessageSenderCache.class).getBean(ProducerCacheProvider.class)
					.isExactlyInstanceOf(CaffeineProducerCacheProvider.class);
		}

	}

}

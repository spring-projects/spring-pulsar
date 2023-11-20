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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.EnableReactivePulsar;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerCustomizerTests.WithMultipleListenersAndSingleCustomizer.WithMultipleListenersAndSingleCustomizerConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerCustomizerTests.WithSingleListenerAndMultipleCustomizers.WithSingleListenerAndMultipleCustomizersConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerCustomizerTests.WithSingleListenerAndSingleCustomizer.WithSingleListenerAndSingleCustomizerConfig;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Tests for the customizers on the {@link ReactivePulsarListener} annotation.
 *
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
@SuppressWarnings({ "unchecked", "rawtypes" })
class ReactivePulsarListenerCustomizerTests implements PulsarTestContainerSupport {

	private ObjectAssert<DefaultReactivePulsarMessageListenerContainer> assertContainer(
			ReactivePulsarListenerEndpointRegistry registry, String containerId) {
		return assertThat(registry.getListenerContainer(containerId)).isNotNull()
			.isInstanceOf(DefaultReactivePulsarMessageListenerContainer.class)
			.asInstanceOf(InstanceOfAssertFactories.type(DefaultReactivePulsarMessageListenerContainer.class));
	}

	@Configuration(proxyBeanMethods = false)
	@EnableReactivePulsar
	static class TopLevelConfig {

		@Bean
		PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarProducerFactory<>(pulsarClient);
		}

		@Bean
		PulsarClient pulsarClient() throws PulsarClientException {
			return new DefaultPulsarClientFactory(PulsarTestContainerSupport.getPulsarBrokerUrl()).createClient();
		}

		@Bean
		ReactivePulsarClient pulsarReactivePulsarClient(PulsarClient pulsarClient) {
			return AdaptedReactivePulsarClientFactory.create(pulsarClient);
		}

		@Bean
		PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			return new PulsarTemplate<>(pulsarProducerFactory);
		}

		@Bean
		ReactivePulsarConsumerFactory<String> pulsarConsumerFactory(ReactivePulsarClient pulsarClient) {
			return new DefaultReactivePulsarConsumerFactory<>(pulsarClient, Collections.emptyList());
		}

		@Bean
		ReactivePulsarListenerContainerFactory<String> reactivePulsarListenerContainerFactory(
				ReactivePulsarConsumerFactory<String> pulsarConsumerFactory) {
			return new DefaultReactivePulsarListenerContainerFactory<>(pulsarConsumerFactory,
					new ReactivePulsarContainerProperties<>());
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PulsarTestContainerSupport.getHttpServiceUrl());
		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleListenerAndSingleCustomizerConfig.class)
	class WithSingleListenerAndSingleCustomizer {

		private static ReactivePulsarListenerMessageConsumerBuilderCustomizer<Object> MY_CUSTOMIZER = mock(
				ReactivePulsarListenerMessageConsumerBuilderCustomizer.class);

		@Test
		void customizerIsAutoAssociated(@Autowired ReactivePulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "singleListenerSingleCustomizer-id").satisfies((container) -> {
				var builder = mock(ReactiveMessageConsumerBuilder.class);
				container.getConsumerCustomizer().customize(builder);
				verify(MY_CUSTOMIZER).customize(builder);
			});
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleListenerAndSingleCustomizerConfig {

			@ReactivePulsarListener(id = "singleListenerSingleCustomizer-id",
					topics = "singleListenerSingleCustomizer-topic")
			void listen(String ignored) {
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> myCustomizer() {
				return MY_CUSTOMIZER;
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithMultipleListenersAndSingleCustomizerConfig.class)
	class WithMultipleListenersAndSingleCustomizer {

		private static ReactivePulsarListenerMessageConsumerBuilderCustomizer<Object> MY_CUSTOMIZER = mock(
				ReactivePulsarListenerMessageConsumerBuilderCustomizer.class);

		@Test
		void customizerIsNotAutoAssociated(@Autowired ReactivePulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "multiListenerSingleCustomizer1-id").satisfies((container) -> {
				var builder = mock(ReactiveMessageConsumerBuilder.class);
				container.getConsumerCustomizer().customize(builder);
				verify(MY_CUSTOMIZER, never()).customize(builder);
			});
			assertContainer(registry, "multiListenerSingleCustomizer2-id").satisfies((container) -> {
				var builder = mock(ReactiveMessageConsumerBuilder.class);
				container.getConsumerCustomizer().customize(builder);
				verify(MY_CUSTOMIZER, never()).customize(builder);
			});
		}

		@Configuration(proxyBeanMethods = false)
		static class WithMultipleListenersAndSingleCustomizerConfig {

			@ReactivePulsarListener(id = "multiListenerSingleCustomizer1-id",
					topics = "multiListenerSingleCustomizer1-topic")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(id = "multiListenerSingleCustomizer2-id",
					topics = "multiListenerSingleCustomizer2-topic")
			void listen2(String ignored) {
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> myCustomizer() {
				return MY_CUSTOMIZER;
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleListenerAndMultipleCustomizersConfig.class)
	class WithSingleListenerAndMultipleCustomizers {

		private static ReactivePulsarListenerMessageConsumerBuilderCustomizer<Object> MY_CUSTOMIZER = mock(
				ReactivePulsarListenerMessageConsumerBuilderCustomizer.class);

		private static ReactivePulsarListenerMessageConsumerBuilderCustomizer<Object> MY_CUSTOMIZER2 = mock(
				ReactivePulsarListenerMessageConsumerBuilderCustomizer.class);

		@Test
		void customizerIsNotAutoAssociated(@Autowired ReactivePulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "singleListenerMultiCustomizers-id").satisfies((container) -> {
				var builder = mock(ReactiveMessageConsumerBuilder.class);
				container.getConsumerCustomizer().customize(builder);
				verify(MY_CUSTOMIZER, never()).customize(builder);
				verify(MY_CUSTOMIZER2, never()).customize(builder);
			});
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleListenerAndMultipleCustomizersConfig {

			@ReactivePulsarListener(id = "singleListenerMultiCustomizers-id",
					topics = "singleListenerMultiCustomizers-topic")
			void listen(String ignored) {
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> myCustomizer1() {
				return MY_CUSTOMIZER;
			}

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> myCustomizer2() {
				return MY_CUSTOMIZER2;
			}

		}

	}

}

/*
 * Copyright 2022-present the original author or authors.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.annotation.PulsarListenerConsumerBuilderCustomizer;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.listener.PulsarListenerCustomizerTests.WithCustomizerOnListener.WithCustomizerOnListenerConfig;
import org.springframework.pulsar.listener.PulsarListenerCustomizerTests.WithMultipleListenersAndSingleCustomizer.WithMultipleListenersAndSingleCustomizerConfig;
import org.springframework.pulsar.listener.PulsarListenerCustomizerTests.WithSingleListenerAndMultipleCustomizers.WithSingleListenerAndMultipleCustomizersConfig;
import org.springframework.pulsar.listener.PulsarListenerCustomizerTests.WithSingleListenerAndSingleCustomizer.WithSingleListenerAndSingleCustomizerConfig;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests setting the consumer customizer on the {@link PulsarListener @PulsarListener}.
 *
 * @author Chris Bono
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class PulsarListenerCustomizerTests extends PulsarListenerTestsBase {

	private ObjectAssert<AbstractPulsarMessageListenerContainer> assertContainer(
			PulsarListenerEndpointRegistry registry, String containerId) {
		return assertThat(registry.getListenerContainer(containerId)).isNotNull()
			.isInstanceOf(AbstractPulsarMessageListenerContainer.class)
			.asInstanceOf(InstanceOfAssertFactories.type(AbstractPulsarMessageListenerContainer.class));
	}

	@Nested
	@ContextConfiguration(classes = WithCustomizerOnListenerConfig.class)
	class WithCustomizerOnListener {

		private static final CountDownLatch latch = new CountDownLatch(1);

		@Test
		void overridesDefaultCustomizer() throws Exception {
			pulsarTemplate.send("overrides-default-topic", "hello");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithCustomizerOnListenerConfig {

			@PulsarListener(topics = "overrides-default-topic", consumerCustomizer = "myCustomizer")
			void listen(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getConsumerName()).isEqualTo("fromMyCustomizer");
				latch.countDown();
			}

			@Bean
			ConsumerBuilderCustomizer<String> defaultCustomizer() {
				return (cb) -> cb.consumerName("fromDefaultCustomizer");
			}

			@Bean
			PulsarListenerConsumerBuilderCustomizer<String> myCustomizer() {
				return (cb) -> cb.consumerName("fromMyCustomizer");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleListenerAndSingleCustomizerConfig.class)
	class WithSingleListenerAndSingleCustomizer {

		private static PulsarListenerConsumerBuilderCustomizer<Object> MY_CUSTOMIZER = mock(
				PulsarListenerConsumerBuilderCustomizer.class);

		@Test
		void customizerIsAutoAssociated(@Autowired PulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "singleListenerSingleCustomizer-id").satisfies((container) -> {
				var builder = mock(ConsumerBuilder.class);
				container.getConsumerBuilderCustomizer().customize(builder);
				verify(MY_CUSTOMIZER).customize(builder);
			});
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleListenerAndSingleCustomizerConfig {

			@PulsarListener(id = "singleListenerSingleCustomizer-id", topics = "singleListenerSingleCustomizer-topic")
			void listen(String ignored) {
			}

			@Bean
			PulsarListenerConsumerBuilderCustomizer<?> myCustomizer() {
				return MY_CUSTOMIZER;
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithMultipleListenersAndSingleCustomizerConfig.class)
	class WithMultipleListenersAndSingleCustomizer {

		@Test
		void customizerIsNotAutoAssociated(@Autowired PulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "multiListenerSingleCustomizer1-id")
				.extracting(AbstractPulsarMessageListenerContainer::getConsumerBuilderCustomizer)
				.isNull();
			assertContainer(registry, "multiListenerSingleCustomizer2-id")
				.extracting(AbstractPulsarMessageListenerContainer::getConsumerBuilderCustomizer)
				.isNull();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithMultipleListenersAndSingleCustomizerConfig {

			@PulsarListener(id = "multiListenerSingleCustomizer1-id", topics = "multiListenerSingleCustomizer1-topic")
			void listen1(String ignored) {
			}

			@PulsarListener(id = "multiListenerSingleCustomizer2-id", topics = "multiListenerSingleCustomizer2-topic")
			void listen2(String ignored) {
			}

			@Bean
			PulsarListenerConsumerBuilderCustomizer<?> myCustomizer() {
				return mock(PulsarListenerConsumerBuilderCustomizer.class);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleListenerAndMultipleCustomizersConfig.class)
	class WithSingleListenerAndMultipleCustomizers {

		@Test
		void customizerIsNotAutoAssociated(@Autowired PulsarListenerEndpointRegistry registry) {
			assertContainer(registry, "singleListenerMultiCustomizers-id")
				.extracting(AbstractPulsarMessageListenerContainer::getConsumerBuilderCustomizer)
				.isNull();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleListenerAndMultipleCustomizersConfig {

			@PulsarListener(id = "singleListenerMultiCustomizers-id", topics = "singleListenerMultiCustomizers-topic")
			void listen(String ignored) {
			}

			@Bean
			PulsarListenerConsumerBuilderCustomizer<?> myCustomizer1() {
				return mock(PulsarListenerConsumerBuilderCustomizer.class);
			}

			@Bean
			PulsarListenerConsumerBuilderCustomizer<?> myCustomizer2() {
				return mock(PulsarListenerConsumerBuilderCustomizer.class);
			}

		}

	}

}

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

package org.springframework.pulsar.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ReaderBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.annotation.PulsarReaderReaderBuilderCustomizer;
import org.springframework.pulsar.config.PulsarReaderEndpointRegistry;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.pulsar.reader.PulsarReaderCustomizerTests.WithCustomizerOnReader.WithCustomizerOnReaderConfig;
import org.springframework.pulsar.reader.PulsarReaderCustomizerTests.WithMultipleReadersAndSingleCustomizer.WithMultipleReadersAndSingleCustomizerConfig;
import org.springframework.pulsar.reader.PulsarReaderCustomizerTests.WithSingleReaderAndMultipleCustomizers.WithSingleReaderAndMultipleCustomizersConfig;
import org.springframework.pulsar.reader.PulsarReaderCustomizerTests.WithSingleReaderAndSingleCustomizer.WithSingleReaderAndSingleCustomizerConfig;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests setting the consumer customizer on the {@link PulsarReader @PulsarReader}.
 *
 * @author Chris Bono
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class PulsarReaderCustomizerTests extends PulsarReaderTestsBase {

	private ObjectAssert<AbstractPulsarMessageReaderContainer> assertContainer(PulsarReaderEndpointRegistry registry,
			String containerId) {
		return assertThat(registry.getReaderContainer(containerId)).isNotNull()
			.isInstanceOf(AbstractPulsarMessageReaderContainer.class)
			.asInstanceOf(InstanceOfAssertFactories.type(AbstractPulsarMessageReaderContainer.class));
	}

	@Nested
	@ContextConfiguration(classes = WithCustomizerOnReaderConfig.class)
	class WithCustomizerOnReader {

		private static final CountDownLatch latch = new CountDownLatch(1);

		@Test
		void overridesDefaultCustomizer() throws Exception {
			pulsarTemplate.send("myCustomizerTopic", "hello");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithCustomizerOnReaderConfig {

			@PulsarReader(id = "overrides-default-id", readerCustomizer = "myCustomizer", startMessageId = "earliest")
			void listen(String ignored) {
				latch.countDown();
			}

			@Bean
			ReaderBuilderCustomizer<String> defaultCustomizer() {
				return (rb) -> rb.topic("defaultCustomizerTopic");
			}

			@Bean
			PulsarReaderReaderBuilderCustomizer<String> myCustomizer() {
				return (rb) -> rb.topic("myCustomizerTopic");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleReaderAndSingleCustomizerConfig.class)
	class WithSingleReaderAndSingleCustomizer {

		private static PulsarReaderReaderBuilderCustomizer<Object> MY_CUSTOMIZER = mock(
				PulsarReaderReaderBuilderCustomizer.class);

		@Test
		void customizerIsAutoAssociated(@Autowired PulsarReaderEndpointRegistry registry) {
			assertContainer(registry, "singleListenerSingleCustomizer-id").satisfies((container) -> {
				var builder = mock(ReaderBuilder.class);
				container.getReaderBuilderCustomizer().customize(builder);
				verify(MY_CUSTOMIZER).customize(builder);
			});
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleReaderAndSingleCustomizerConfig {

			@PulsarReader(id = "singleListenerSingleCustomizer-id", topics = "singleListenerSingleCustomizer-topic",
					startMessageId = "earliest")
			void listen(String ignored) {
			}

			@Bean
			PulsarReaderReaderBuilderCustomizer<?> myCustomizer() {
				return MY_CUSTOMIZER;
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithMultipleReadersAndSingleCustomizerConfig.class)
	class WithMultipleReadersAndSingleCustomizer {

		@Test
		void customizerIsNotAutoAssociated(@Autowired PulsarReaderEndpointRegistry registry) {
			assertContainer(registry, "multiReaderSingleCustomizer1-id")
				.extracting(AbstractPulsarMessageReaderContainer::getReaderBuilderCustomizer)
				.isNull();
			assertContainer(registry, "multiReaderSingleCustomizer2-id")
				.extracting(AbstractPulsarMessageReaderContainer::getReaderBuilderCustomizer)
				.isNull();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithMultipleReadersAndSingleCustomizerConfig {

			@PulsarReader(id = "multiReaderSingleCustomizer1-id", topics = "multiReaderSingleCustomizer1-topic",
					startMessageId = "earliest")
			void listen1(String ignored) {
			}

			@PulsarReader(id = "multiReaderSingleCustomizer2-id", topics = "multiReaderSingleCustomizer2-topic",
					startMessageId = "earliest")
			void listen2(String ignored) {
			}

			@Bean
			PulsarReaderReaderBuilderCustomizer<?> myCustomizer() {
				return mock(PulsarReaderReaderBuilderCustomizer.class);
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithSingleReaderAndMultipleCustomizersConfig.class)
	class WithSingleReaderAndMultipleCustomizers {

		@Test
		void customizerIsNotAutoAssociated(@Autowired PulsarReaderEndpointRegistry registry) {
			assertContainer(registry, "singleReaderMultiCustomizers-id")
				.extracting(AbstractPulsarMessageReaderContainer::getReaderBuilderCustomizer)
				.isNull();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithSingleReaderAndMultipleCustomizersConfig {

			@PulsarReader(id = "singleReaderMultiCustomizers-id", topics = "singleReaderMultiCustomizers-topic",
					startMessageId = "earliest")
			void listen(String ignored) {
			}

			@Bean
			PulsarReaderReaderBuilderCustomizer<?> myCustomizer1() {
				return mock(PulsarReaderReaderBuilderCustomizer.class);
			}

			@Bean
			PulsarReaderReaderBuilderCustomizer<?> myCustomizer2() {
				return mock(PulsarReaderReaderBuilderCustomizer.class);
			}

		}

	}

}

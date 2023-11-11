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
import static org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSubscriptionNameTests.WithNameGenerator.WithNameGeneratorConfig;
import static org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSubscriptionNameTests.WithoutNameGenerator.WithoutNameGeneratorConfig;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.config.PulsarEndpointSubscriptionNameGenerator;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.test.context.ContextConfiguration;

import reactor.core.publisher.Mono;

/**
 * Tests for the subscription name setting on the
 * {@link ReactivePulsarListener @ReactivePulsarListener}.
 *
 * @author Chris Bono
 */
class ReactivePulsarListenerSubscriptionNameTests extends ReactivePulsarListenerTestsBase {

	@Nested
	@ContextConfiguration(classes = WithoutNameGeneratorConfig.class)
	class WithoutNameGenerator {

		static final CountDownLatch latchNameNotSet = new CountDownLatch(1);
		static final CountDownLatch latchNameSet = new CountDownLatch(1);
		static final CountDownLatch latchNameExprSet = new CountDownLatch(1);
		static final CountDownLatch latchWithCustomizer = new CountDownLatch(1);

		@Autowired
		protected ReactivePulsarListenerEndpointRegistry<String> registry;

		@Test
		void whenNameNotSetOnAnnotationThenDefaultGeneratedNameIsUsed() throws Exception {
			assertSubscriptionName("nameNotSet-id")
				.startsWith("org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#");
			pulsarTemplate.send("nameNotSet-topic", "hello-nameNotSet");
			assertThat(latchNameNotSet.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameSetOnAnnotationThenSpecifiedNameIsUsed() throws Exception {
			assertSubscriptionName("nameSet-id").isEqualTo("nameSet-sub");
			pulsarTemplate.send("nameSet-topic", "hello-nameSet");
			assertThat(latchNameSet.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameExprSetOnAnnotationThenEvaluatedNameIsUsed() throws Exception {
			assertSubscriptionName("nameExprSet-id").isEqualTo("nameExprSet-sub");
			pulsarTemplate.send("nameExprSet-topic", "hello-nameExprSet");
			assertThat(latchNameExprSet.await(5, TimeUnit.SECONDS)).isTrue();
		}

		private AbstractStringAssert<?> assertSubscriptionName(String containerId) {
			return assertThat(registry.getListenerContainer(containerId).getContainerProperties())
				.extracting(ReactivePulsarContainerProperties::getSubscriptionName, InstanceOfAssertFactories.STRING);
		}

		@Configuration(proxyBeanMethods = false)
		static class WithoutNameGeneratorConfig {

			@ReactivePulsarListener(id = "nameNotSet-id", topics = "nameNotSet-topic")
			Mono<Void> listenWithoutNameSet(String ignored) {
				latchNameNotSet.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "nameSet-id", topics = "nameSet-topic", subscriptionName = "nameSet-sub")
			Mono<Void> listenWithNameSet(String ignored) {
				latchNameSet.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "nameExprSet-id", topics = "nameExprSet-topic",
					subscriptionName = "#{'nameExprSet'.concat('-sub')}")
			Mono<Void> listenWithNameExpressionSet(String ignored) {
				latchNameExprSet.countDown();
				return Mono.empty();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithNameGeneratorConfig.class)
	class WithNameGenerator {

		static final CountDownLatch latchNameGenWithNameSet = new CountDownLatch(1);
		static final CountDownLatch latchNameGenOnly = new CountDownLatch(1);
		static final CountDownLatch latchNameGenEmpty = new CountDownLatch(1);

		@Autowired
		protected ReactivePulsarListenerEndpointRegistry<String> registry;

		@Test
		void whenNameSetOnAnnotationThenSpecifiedNameTakesPrecedence() throws Exception {
			assertSubscriptionName("nameGenWithNameSet-id").isEqualTo("nameGenWithNameSet-sub");
			pulsarTemplate.send("nameGenWithNameSet-topic", "hello-nameGenWithNameSet");
			assertThat(latchNameGenWithNameSet.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameNotSetOnAnnotationThenGeneratedNameIsUsed() throws Exception {
			assertSubscriptionName("nameGenOnly-id").isEqualTo("nameGenOnly-id-generated-sub");
			pulsarTemplate.send("nameGenOnly-topic", "hello-nameGenOnly");
			assertThat(latchNameGenOnly.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameGeneratorReturnsEmptyThenDefaultGeneratedNameIsUsed() throws Exception {
			assertSubscriptionName("nameGenEmpty-id")
				.startsWith("org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#");
			pulsarTemplate.send("nameGenEmpty-topic", "hello-nameGenEmpty");
			assertThat(latchNameGenEmpty.await(5, TimeUnit.SECONDS)).isTrue();
		}

		private AbstractStringAssert<?> assertSubscriptionName(String containerId) {
			return assertThat(registry.getListenerContainer(containerId).getContainerProperties())
				.extracting(ReactivePulsarContainerProperties::getSubscriptionName, InstanceOfAssertFactories.STRING);
		}

		@Configuration(proxyBeanMethods = false)
		static class WithNameGeneratorConfig {

			@ReactivePulsarListener(id = "nameGenWithNameSet-id", topics = "nameGenWithNameSet-topic",
					subscriptionName = "nameGenWithNameSet-sub")
			Mono<Void> listenWithGeneratorAndNameSet(String ignored) {
				latchNameGenWithNameSet.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "nameGenOnly-id", topics = "nameGenOnly-topic")
			Mono<Void> listenWithGenerator(String ignored) {
				latchNameGenOnly.countDown();
				return Mono.empty();
			}

			@ReactivePulsarListener(id = "nameGenEmpty-id", topics = "nameGenEmpty-topic")
			Mono<Void> listenWithGeneratorEmptyName(String ignored) {
				latchNameGenEmpty.countDown();
				return Mono.empty();
			}

			@Configuration(proxyBeanMethods = false)
			static class NameGeneratorConfig {

				@Bean
				PulsarEndpointSubscriptionNameGenerator<ReactivePulsarListener> nameGenerator() {
					return endpointSpecifier -> {
						var listenerId = endpointSpecifier.id();
						if ("nameGenEmpty-id".equals(listenerId)) {
							return Optional.empty();
						}
						return Optional.of(listenerId + "-generated-sub");
					};
				}

			}

		}

	}

}

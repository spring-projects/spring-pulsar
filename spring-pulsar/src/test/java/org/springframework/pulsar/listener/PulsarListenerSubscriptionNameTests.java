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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.pulsar.listener.PulsarListenerSubscriptionNameTests.WithNameGenerator.WithNameGeneratorConfig;
import static org.springframework.pulsar.listener.PulsarListenerSubscriptionNameTests.WithoutNameGenerator.WithoutNameGeneratorConfig;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.PulsarEndpointSubscriptionNameGenerator;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for the subscription name setting on the {@link PulsarListener @PulsarListener}.
 *
 * @author Chris Bono
 */
class PulsarListenerSubscriptionNameTests extends PulsarListenerTestsBase {

	@Nested
	@ContextConfiguration(classes = WithoutNameGeneratorConfig.class)
	class WithoutNameGenerator {

		static final CountDownLatch latchNameNotSet = new CountDownLatch(1);
		static final CountDownLatch latchNameSet = new CountDownLatch(1);
		static final CountDownLatch latchNameExprSet = new CountDownLatch(1);
		static final CountDownLatch latchWithCustomizer = new CountDownLatch(1);

		@Test
		void whenNameNotSetOnAnnotationThenDefaultGeneratedNameIsUsed() throws Exception {
			pulsarTemplate.send("nameNotSet-topic", "hello-nameNotSet");
			assertThat(latchNameNotSet.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameSetOnAnnotationThenSpecifiedNameIsUsed() throws Exception {
			pulsarTemplate.send("nameSet-topic", "hello-nameSet");
			assertThat(latchNameSet.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameExprSetOnAnnotationThenEvaluatedNameIsUsed() throws Exception {
			pulsarTemplate.send("nameExprSet-topic", "hello-nameExprSet");
			assertThat(latchNameExprSet.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameCustomizerIsConfiguredThenCustomizerTakesPrecedence() throws Exception {
			pulsarTemplate.send("withCustomizer-topic", "hello-withCustomizer");
			assertThat(latchWithCustomizer.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithoutNameGeneratorConfig {

			@PulsarListener(topics = "nameNotSet-topic")
			void listenWithoutNameSet(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription())
					.startsWith("org.springframework.Pulsar.PulsarListenerEndpointContainer#");
				latchNameNotSet.countDown();
			}

			@PulsarListener(topics = "nameSet-topic", subscriptionName = "nameSet-sub")
			void listenWithNameSet(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription()).isEqualTo("nameSet-sub");
				latchNameSet.countDown();
			}

			@PulsarListener(topics = "nameExprSet-topic", subscriptionName = "#{'nameExprSet'.concat('-sub')}")
			void listenWithNameExpressionSet(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription()).isEqualTo("nameExprSet-sub");
				latchNameExprSet.countDown();
			}

			@PulsarListener(topics = "withCustomizer-topic", subscriptionName = "withCustomizer-sub",
					consumerCustomizer = "myCustomizer")
			void listenWithCustomizer(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription()).isEqualTo("customizerSet-sub");
				latchWithCustomizer.countDown();
			}

			@Bean
			public ConsumerBuilderCustomizer<String> myCustomizer() {
				return cb -> cb.subscriptionName("customizerSet-sub");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithNameGeneratorConfig.class)
	class WithNameGenerator {

		static final CountDownLatch latchNameGenWithNameSet = new CountDownLatch(1);
		static final CountDownLatch latchNameGenOnly = new CountDownLatch(1);
		static final CountDownLatch latchNameGenEmpty = new CountDownLatch(1);

		@Test
		void whenNameSetOnAnnotationThenSpecifiedNameTakesPrecedence() throws Exception {
			pulsarTemplate.send("nameGenWithNameSet-topic", "hello-nameGenWithNameSet");
			assertThat(latchNameGenWithNameSet.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameNotSetOnAnnotationThenGeneratedNameIsUsed() throws Exception {
			pulsarTemplate.send("nameGenOnly-topic", "hello-nameGenOnly");
			assertThat(latchNameGenOnly.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Test
		void whenNameGeneratorReturnsEmptyThenDefaultGeneratedNameIsUsed() throws Exception {
			pulsarTemplate.send("nameGenEmpty-topic", "hello-nameGenEmpty");
			assertThat(latchNameGenEmpty.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithNameGeneratorConfig {

			@PulsarListener(id = "5150", topics = "nameGenWithNameSet-topic",
					subscriptionName = "nameGenWithNameSet-sub")
			void listenWithGeneratorAndNameSet(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription()).isEqualTo("nameGenWithNameSet-sub");
				latchNameGenWithNameSet.countDown();
			}

			@PulsarListener(id = "6160", topics = "nameGenOnly-topic")
			void listenWithGenerator(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription()).isEqualTo("6160-sub");
				latchNameGenOnly.countDown();
			}

			@PulsarListener(id = "7170", topics = "nameGenEmpty-topic")
			void listenWithGeneratorEmptyName(String ignored, Consumer<String> consumer) {
				assertThat(consumer.getSubscription())
					.startsWith("org.springframework.Pulsar.PulsarListenerEndpointContainer#");
				latchNameGenEmpty.countDown();
			}

			@Configuration(proxyBeanMethods = false)
			static class NameGeneratorConfig {

				@Bean
				PulsarEndpointSubscriptionNameGenerator<PulsarListener> nameGenerator() {
					return endpointSpecifier -> {
						var listenerId = endpointSpecifier.id();
						if ("7170".equals(listenerId)) {
							return Optional.empty();
						}
						return Optional.of(listenerId + "-sub");
					};
				}

			}

		}

	}

}

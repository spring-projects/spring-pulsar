/*
 * Copyright 2022-2024 the original author or authors.
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.AutoStartupAttribute.AutoStartupAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.ConcurrencyAttribute.ConcurrencyAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.ConsumerCustomizerAttribute.ConsumerCustomizerAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.ContainerFactoryAttribute.ContainerFactoryAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.DeadLetterPolicyAttribute.DeadLetterPolicyAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.IdAttribute.IdAttributeConfig;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerSpelTests.UseKeyOrderedProcessingAttribute.UseKeyOrderedProcessingAttributeConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests {@code SpEL} functionality in
 * {@link ReactivePulsarListener @ReactivePulsarListener} attributes.
 *
 * @author Chris Bono
 */
class ReactivePulsarListenerSpelTests extends ReactivePulsarListenerTestsBase {

	private static final String TOPIC = "pulsar-reactive-listener-spel-tests-topic";

	@Nested
	@ContextConfiguration(classes = IdAttributeConfig.class)
	@TestPropertySource(properties = "foo.id = foo")
	class IdAttribute {

		@Test
		void containerIdDerivedFromAttribute(@Autowired ReactivePulsarListenerEndpointRegistry<String> registry) {
			assertThat(registry.getListenerContainer("foo")).isNotNull();
			assertThat(registry.getListenerContainer("bar")).isNotNull();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class IdAttributeConfig {

			@ReactivePulsarListener(topics = TOPIC, id = "${foo.id}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "#{T(java.lang.String).valueOf('bar')}")
			void listen2(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ContainerFactoryAttributeConfig.class)
	class ContainerFactoryAttribute {

		@Test
		void containerFactoryDerivedFromAttribute(
				@Autowired ReactivePulsarListenerContainerFactory<String> containerFactory) {
			verify(containerFactory).createListenerContainer(argThat(endpoint -> endpoint.getId().equals("foo")));
			verify(containerFactory).createListenerContainer(argThat(endpoint -> endpoint.getId().equals("bar")));
			verify(containerFactory).createListenerContainer(argThat(endpoint -> endpoint.getId().equals("zaa")));
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ContainerFactoryAttributeConfig {

			@SuppressWarnings({ "unchecked", "SpringJavaInjectionPointsAutowiringInspection" })
			@Bean
			@Primary
			ReactivePulsarListenerContainerFactory<String> customContainerFactory(
					ReactivePulsarConsumerFactory<String> pulsarConsumerFactory) {
				return spy(new DefaultReactivePulsarListenerContainerFactory<>(pulsarConsumerFactory,
						new ReactivePulsarContainerProperties<>()));
			}

			@ReactivePulsarListener(topics = TOPIC, id = "foo", containerFactory = "#{@customContainerFactory}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "bar",
					containerFactory = "#{T(java.lang.String).valueOf('customContainerFactory')}")
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "zaa", containerFactory = "customContainerFactory")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = AutoStartupAttributeConfig.class)
	@TestPropertySource(properties = "foo.auto-start = true")
	class AutoStartupAttribute {

		@Test
		void containerAutoStartupDerivedFromAttribute(
				@Autowired ReactivePulsarListenerEndpointRegistry<String> registry) {
			assertThat(registry.getListenerContainer("foo").isAutoStartup()).isTrue();
			assertThat(registry.getListenerContainer("bar").isAutoStartup()).isFalse();
			assertThat(registry.getListenerContainer("zaa").isAutoStartup()).isTrue();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class AutoStartupAttributeConfig {

			@ReactivePulsarListener(topics = TOPIC, id = "foo", autoStartup = "${foo.auto-start}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "bar",
					autoStartup = "#{T(java.lang.Boolean).valueOf('false')}")
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "zaa", autoStartup = "true")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ConcurrencyAttributeConfig.class)
	@TestPropertySource(properties = "foo.concurrency = 2")
	class ConcurrencyAttribute {

		@Test
		void containerAutoStartupDerivedFromAttribute(
				@Autowired ReactivePulsarListenerEndpointRegistry<String> registry) {
			assertThat(registry.getListenerContainer("foo").getContainerProperties().getConcurrency()).isEqualTo(2);
			assertThat(registry.getListenerContainer("bar").getContainerProperties().getConcurrency()).isEqualTo(3);
			assertThat(registry.getListenerContainer("zaa").getContainerProperties().getConcurrency()).isEqualTo(4);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ConcurrencyAttributeConfig {

			@ReactivePulsarListener(topics = TOPIC, id = "foo", concurrency = "${foo.concurrency}",
					subscriptionType = SubscriptionType.Shared)
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "bar", concurrency = "#{T(java.lang.Integer).valueOf('3')}",
					subscriptionType = SubscriptionType.Shared)
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "zaa", concurrency = "4",
					subscriptionType = SubscriptionType.Shared)
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = UseKeyOrderedProcessingAttributeConfig.class)
	@TestPropertySource(properties = "foo.key-ordered = true")
	class UseKeyOrderedProcessingAttribute {

		@Test
		void containerUseKeyOrderedProcessingDerivedFromAttribute(
				@Autowired ReactivePulsarListenerEndpointRegistry<String> registry) {
			assertThat(registry.getListenerContainer("foo").getContainerProperties().isUseKeyOrderedProcessing())
				.isTrue();
			assertThat(registry.getListenerContainer("bar").getContainerProperties().isUseKeyOrderedProcessing())
				.isFalse();
			assertThat(registry.getListenerContainer("zaa").getContainerProperties().isUseKeyOrderedProcessing())
				.isTrue();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class UseKeyOrderedProcessingAttributeConfig {

			@ReactivePulsarListener(topics = TOPIC, id = "foo", useKeyOrderedProcessing = "${foo.key-ordered}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "bar",
					useKeyOrderedProcessing = "#{T(java.lang.Boolean).valueOf('false')}")
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "zaa", useKeyOrderedProcessing = "true")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = DeadLetterPolicyAttributeConfig.class)
	class DeadLetterPolicyAttribute {

		@Test
		void deadLetterPolicyDerivedFromAttribute(@Autowired ReactivePulsarListenerEndpointRegistry<String> registry) {
			assertDeadLetterPolicy(registry, "foo");
			assertDeadLetterPolicy(registry, "bar");
			assertDeadLetterPolicy(registry, "zaa");
		}

		private void assertDeadLetterPolicy(ReactivePulsarListenerEndpointRegistry<String> registry,
				String containerId) {
			assertThat(registry.getListenerContainer(containerId)).extracting("pulsarConsumerFactory")
				.extracting("topicNameToConsumerSpec",
						InstanceOfAssertFactories.map(String.class, ReactiveMessageConsumerSpec.class))
				.extractingByKey("%s-topic".formatted(containerId))
				.extracting(ReactiveMessageConsumerSpec::getDeadLetterPolicy)
				.isSameAs(DeadLetterPolicyAttributeConfig.CUSTOM_DLP);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class DeadLetterPolicyAttributeConfig {

			static DeadLetterPolicy CUSTOM_DLP = DeadLetterPolicy.builder()
				.deadLetterTopic("dlt")
				.maxRedeliverCount(2)
				.build();

			@Bean
			DeadLetterPolicy customDeadLetterPolicy() {
				return CUSTOM_DLP;
			}

			@ReactivePulsarListener(id = "foo", topics = "foo-topic", deadLetterPolicy = "#{@customDeadLetterPolicy}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(id = "bar", topics = "bar-topic",
					deadLetterPolicy = "#{T(java.lang.String).valueOf('customDeadLetterPolicy')}")
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(id = "zaa", topics = "zaa-topic", deadLetterPolicy = "customDeadLetterPolicy")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ConsumerCustomizerAttributeConfig.class)
	class ConsumerCustomizerAttribute {

		@Test
		void consumerCustomizerDerivedFromAttribute() {
			assertThat(ConsumerCustomizerAttributeConfig.CUSTOMIZED_CONTAINERS_SUBSCRIPTION_NAMES)
				.containsExactlyInAnyOrder("fooSub", "barSub", "zaaSub");
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ConsumerCustomizerAttributeConfig {

			static List<String> CUSTOMIZED_CONTAINERS_SUBSCRIPTION_NAMES = new ArrayList<>();

			@Bean
			ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> customConsumerCustomizer() {
				return (builder) -> {
					var conf = ReflectionTestUtils.getField(builder, "consumerSpec");
					assertThat(conf).isNotNull();
					CUSTOMIZED_CONTAINERS_SUBSCRIPTION_NAMES
						.add(Objects.toString(ReflectionTestUtils.getField(conf, "subscriptionName"), "???"));
				};
			}

			@ReactivePulsarListener(topics = TOPIC, id = "foo", subscriptionName = "fooSub",
					consumerCustomizer = "#{@customConsumerCustomizer}")
			void listen1(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "bar", subscriptionName = "barSub",
					consumerCustomizer = "#{T(java.lang.String).valueOf('customConsumerCustomizer')}")
			void listen2(String ignored) {
			}

			@ReactivePulsarListener(topics = TOPIC, id = "zaa", subscriptionName = "zaaSub",
					consumerCustomizer = "customConsumerCustomizer")
			void listen3(String ignored) {
			}

		}

	}

}

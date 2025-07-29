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
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.annotation.PulsarListenerConsumerBuilderCustomizer;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpoint;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.AckRedeliveryBackoffAttribute.AckRedeliveryBackoffAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.AutoStartupAttribute.AutoStartupAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.ConcurrencyAttribute.ConcurrencyAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.ConsumerCustomizerAttribute.ConsumerCustomizerAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.ConsumerErrorHandlerAttribute.ConsumerErrorHandlerAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.ContainerFactoryAttribute.ContainerFactoryAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.DeadLetterPolicyAttribute.DeadLetterPolicyAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.IdAttribute.IdAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.NackRedeliveryBackoffAttribute.NackRedeliveryBackoffAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.PropertiesAttribute.PropertiesAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.SubscriptionNameAttribute.SubscriptionNameAttributeConfig;
import org.springframework.pulsar.listener.PulsarListenerSpelTests.TopicsAttribute.TopicsAttributeConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests {@code SpEL} functionality in {@link PulsarListener @PulsarListener} attributes.
 *
 * @author Chris Bono
 */
class PulsarListenerSpelTests extends PulsarListenerTestsBase {

	private static final String TOPIC = "pulsar-listener-spel-tests-topic";

	@Nested
	@ContextConfiguration(classes = IdAttributeConfig.class)
	@TestPropertySource(properties = "foo.id = foo")
	class IdAttribute {

		@Test
		void containerIdDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).isNotNull();
			assertThat(registry.getListenerContainer("bar")).isNotNull();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class IdAttributeConfig {

			@PulsarListener(topics = TOPIC, id = "${foo.id}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "#{T(java.lang.String).valueOf('bar')}")
			void listen2(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = SubscriptionNameAttributeConfig.class)
	@TestPropertySource(properties = "foo.subscriptionName = fooSub")
	class SubscriptionNameAttribute {

		@Test
		void subscriptionNameDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo").getContainerProperties().getSubscriptionName())
				.isEqualTo("fooSub");
			assertThat(registry.getListenerContainer("bar").getContainerProperties().getSubscriptionName())
				.isEqualTo("barSub");
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class SubscriptionNameAttributeConfig {

			@PulsarListener(topics = TOPIC, id = "foo", subscriptionName = "${foo.subscriptionName}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar", subscriptionName = "#{T(java.lang.String).valueOf('barSub')}")
			void listen2(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = TopicsAttributeConfig.class)
	@TestPropertySource(properties = { "foo.topics = foo", "foo.topicPattern = foo*" })
	class TopicsAttribute {

		@Test
		void topicsDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo").getContainerProperties().getTopics())
				.containsExactly("foo");
			assertThat(registry.getListenerContainer("bar").getContainerProperties().getTopics())
				.containsExactly("bar");
			assertThat(registry.getListenerContainer("zaa").getContainerProperties().getTopicsPattern())
				.isEqualTo("foo*");
			assertThat(registry.getListenerContainer("laa").getContainerProperties().getTopicsPattern())
				.isEqualTo("bar*");
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class TopicsAttributeConfig {

			@PulsarListener(topics = "${foo.topics}", id = "foo")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = "#{T(java.lang.String).valueOf('bar')}", id = "bar")
			void listen2(String ignored) {
			}

			@PulsarListener(topicPattern = "${foo.topicPattern}", id = "zaa")
			void listen3(String ignored) {
			}

			@PulsarListener(topicPattern = "#{T(java.lang.String).valueOf('bar*')}", id = "laa")
			void listen4(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ContainerFactoryAttributeConfig.class)
	class ContainerFactoryAttribute {

		@Test
		void containerFactoryDerivedFromAttribute(@Autowired PulsarListenerContainerFactory containerFactory) {
			verify(containerFactory).createRegisteredContainer(argThat(endpoint -> endpoint.getId().equals("foo")));
			verify(containerFactory).createRegisteredContainer(argThat(endpoint -> endpoint.getId().equals("bar")));
			verify(containerFactory).createRegisteredContainer(argThat(endpoint -> endpoint.getId().equals("zaa")));
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ContainerFactoryAttributeConfig {

			@Bean
			@Primary
			PulsarListenerContainerFactory customContainerFactory() {
				var mockContainerFactory = mock(PulsarListenerContainerFactory.class);
				AbstractPulsarMessageListenerContainer<?> mockContainer = mock(
						AbstractPulsarMessageListenerContainer.class);
				when(mockContainerFactory.createRegisteredContainer(any(PulsarListenerEndpoint.class)))
					.thenReturn(mockContainer);
				return mockContainerFactory;
			}

			@PulsarListener(topics = TOPIC, id = "foo", containerFactory = "#{@customContainerFactory}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					containerFactory = "#{T(java.lang.String).valueOf('customContainerFactory')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", containerFactory = "customContainerFactory")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = AutoStartupAttributeConfig.class)
	@TestPropertySource(properties = "foo.auto-start = true")
	class AutoStartupAttribute {

		@Test
		void containerAutoStartupDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo").isAutoStartup()).isTrue();
			assertThat(registry.getListenerContainer("bar").isAutoStartup()).isFalse();
			assertThat(registry.getListenerContainer("zaa").isAutoStartup()).isTrue();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class AutoStartupAttributeConfig {

			@PulsarListener(topics = TOPIC, id = "foo", autoStartup = "${foo.auto-start}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar", autoStartup = "#{T(java.lang.Boolean).valueOf('false')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", autoStartup = "true")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = PropertiesAttributeConfig.class)
	@TestPropertySource(properties = { "foo.mykey = subscriptionName", "foo.myvalue = fooSub" })
	class PropertiesAttribute {

		@Test
		void containerPropsDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo").getContainerProperties().getPulsarConsumerProperties())
				.containsEntry("subscriptionName", "fooSub");
			assertThat(registry.getListenerContainer("bar").getContainerProperties().getPulsarConsumerProperties())
				.containsEntry("subscriptionName", "barSub");
			assertThat(registry.getListenerContainer("zaa").getContainerProperties().getPulsarConsumerProperties())
				.contains(entry("subscriptionName", "zaaSub"), entry("consumerName", "zaaConsumer"));
			assertThat(registry.getListenerContainer("laa").getContainerProperties().getPulsarConsumerProperties())
				.contains(entry("subscriptionName", "laaSub"), entry("consumerName", "laaConsumer"));
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class PropertiesAttributeConfig {

			@Bean
			String[] stringArray() {
				return new String[] { "subscriptionName=zaaSub", "consumerName=zaaConsumer" };
			}

			@Bean
			List<String> stringList() {
				return List.of("subscriptionName=laaSub", "consumerName=laaConsumer");
			}

			@PulsarListener(topics = TOPIC, id = "foo", properties = { "${foo.mykey}=${foo.myvalue}" })
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					properties = "#{T(java.lang.String).valueOf('subscriptionName=barSub')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", properties = "#{@stringArray}")
			void listen3(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "laa", properties = "#{@stringList}")
			void listen4(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ConcurrencyAttributeConfig.class)
	@TestPropertySource(properties = "foo.concurrency = 2")
	class ConcurrencyAttribute {

		@Test
		void containerAutoStartupDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).hasFieldOrPropertyWithValue("concurrency", 2);
			assertThat(registry.getListenerContainer("bar")).hasFieldOrPropertyWithValue("concurrency", 3);
			assertThat(registry.getListenerContainer("zaa")).hasFieldOrPropertyWithValue("concurrency", 4);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ConcurrencyAttributeConfig {

			@PulsarListener(topics = TOPIC, id = "foo", concurrency = "${foo.concurrency}",
					subscriptionType = SubscriptionType.Shared)
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar", concurrency = "#{T(java.lang.Integer).valueOf('3')}",
					subscriptionType = SubscriptionType.Shared)
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", concurrency = "4", subscriptionType = SubscriptionType.Shared)
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = NackRedeliveryBackoffAttributeConfig.class)
	class NackRedeliveryBackoffAttribute {

		@Test
		void nackRedeliveryBackoffDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).extracting("negativeAckRedeliveryBackoff")
				.isSameAs(NackRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
			assertThat(registry.getListenerContainer("bar")).extracting("negativeAckRedeliveryBackoff")
				.isSameAs(NackRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
			assertThat(registry.getListenerContainer("zaa")).extracting("negativeAckRedeliveryBackoff")
				.isSameAs(NackRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class NackRedeliveryBackoffAttributeConfig {

			static RedeliveryBackoff CUSTOM_BACKOFF = (i) -> i;

			@Bean
			RedeliveryBackoff customNackRedeliveryBackoff() {
				return CUSTOM_BACKOFF;
			}

			@PulsarListener(topics = TOPIC, id = "foo",
					negativeAckRedeliveryBackoff = "#{@customNackRedeliveryBackoff}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					negativeAckRedeliveryBackoff = "#{T(java.lang.String).valueOf('customNackRedeliveryBackoff')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", negativeAckRedeliveryBackoff = "customNackRedeliveryBackoff")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = AckRedeliveryBackoffAttributeConfig.class)
	class AckRedeliveryBackoffAttribute {

		@Test
		void ackRedeliveryBackoffDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).extracting("ackTimeoutRedeliveryBackoff")
				.isSameAs(AckRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
			assertThat(registry.getListenerContainer("bar")).extracting("ackTimeoutRedeliveryBackoff")
				.isSameAs(AckRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
			assertThat(registry.getListenerContainer("zaa")).extracting("ackTimeoutRedeliveryBackoff")
				.isSameAs(AckRedeliveryBackoffAttributeConfig.CUSTOM_BACKOFF);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class AckRedeliveryBackoffAttributeConfig {

			static RedeliveryBackoff CUSTOM_BACKOFF = (i) -> i;

			@Bean
			RedeliveryBackoff customAckRedeliveryBackoff() {
				return CUSTOM_BACKOFF;
			}

			@PulsarListener(topics = TOPIC, id = "foo", ackTimeoutRedeliveryBackoff = "#{@customAckRedeliveryBackoff}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					ackTimeoutRedeliveryBackoff = "#{T(java.lang.String).valueOf('customAckRedeliveryBackoff')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", ackTimeoutRedeliveryBackoff = "customAckRedeliveryBackoff")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = DeadLetterPolicyAttributeConfig.class)
	class DeadLetterPolicyAttribute {

		@Test
		void deadLetterPolicyDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).extracting("deadLetterPolicy")
				.isSameAs(DeadLetterPolicyAttributeConfig.CUSTOM_DLP);
			assertThat(registry.getListenerContainer("bar")).extracting("deadLetterPolicy")
				.isSameAs(DeadLetterPolicyAttributeConfig.CUSTOM_DLP);
			assertThat(registry.getListenerContainer("zaa")).extracting("deadLetterPolicy")
				.isSameAs(DeadLetterPolicyAttributeConfig.CUSTOM_DLP);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class DeadLetterPolicyAttributeConfig {

			static DeadLetterPolicy CUSTOM_DLP = DeadLetterPolicy.builder().deadLetterTopic("dlt").build();

			@Bean
			DeadLetterPolicy customDeadLetterPolicy() {
				return CUSTOM_DLP;
			}

			@PulsarListener(topics = TOPIC, id = "foo", deadLetterPolicy = "#{@customDeadLetterPolicy}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					deadLetterPolicy = "#{T(java.lang.String).valueOf('customDeadLetterPolicy')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", deadLetterPolicy = "customDeadLetterPolicy")
			void listen3(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ConsumerErrorHandlerAttributeConfig.class)
	class ConsumerErrorHandlerAttribute {

		@Test
		void consumerErrorHandlerDerivedFromAttribute(@Autowired PulsarListenerEndpointRegistry registry) {
			assertThat(registry.getListenerContainer("foo")).extracting("pulsarConsumerErrorHandler")
				.isSameAs(ConsumerErrorHandlerAttributeConfig.CUSTOM_ERROR_HANDLER);
			assertThat(registry.getListenerContainer("bar")).extracting("pulsarConsumerErrorHandler")
				.isSameAs(ConsumerErrorHandlerAttributeConfig.CUSTOM_ERROR_HANDLER);
			assertThat(registry.getListenerContainer("zaa")).extracting("pulsarConsumerErrorHandler")
				.isSameAs(ConsumerErrorHandlerAttributeConfig.CUSTOM_ERROR_HANDLER);
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ConsumerErrorHandlerAttributeConfig {

			static PulsarConsumerErrorHandler<?> CUSTOM_ERROR_HANDLER = mock(PulsarConsumerErrorHandler.class);

			@Bean
			PulsarConsumerErrorHandler<?> customConsumerErrorHandler() {
				return CUSTOM_ERROR_HANDLER;
			}

			@PulsarListener(topics = TOPIC, id = "foo", pulsarConsumerErrorHandler = "#{@customConsumerErrorHandler}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar",
					pulsarConsumerErrorHandler = "#{T(java.lang.String).valueOf('customConsumerErrorHandler')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", pulsarConsumerErrorHandler = "customConsumerErrorHandler")
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
			PulsarListenerConsumerBuilderCustomizer<?> customConsumerCustomizer() {
				return (builder) -> {
					var conf = ReflectionTestUtils.getField(builder, "conf");
					assertThat(conf).isNotNull();
					CUSTOMIZED_CONTAINERS_SUBSCRIPTION_NAMES
						.add(Objects.toString(ReflectionTestUtils.getField(conf, "subscriptionName"), "???"));
				};
			}

			@PulsarListener(topics = TOPIC, id = "foo", subscriptionName = "fooSub",
					consumerCustomizer = "#{@customConsumerCustomizer}")
			void listen1(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "bar", subscriptionName = "barSub",
					consumerCustomizer = "#{T(java.lang.String).valueOf('customConsumerCustomizer')}")
			void listen2(String ignored) {
			}

			@PulsarListener(topics = TOPIC, id = "zaa", subscriptionName = "zaaSub",
					consumerCustomizer = "customConsumerCustomizer")
			void listen3(String ignored) {
			}

		}

	}

}

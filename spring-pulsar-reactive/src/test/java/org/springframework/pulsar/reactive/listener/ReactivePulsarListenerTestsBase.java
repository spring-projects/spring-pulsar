/*
 * Copyright 2023 the original author or authors.
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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.annotation.EnableReactivePulsar;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarListenerTests.ConsumerTrackingReactivePulsarConsumerFactory;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Provides base support for {@link ReactivePulsarListener @ReactivePulsarListener} tests.
 *
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
abstract class ReactivePulsarListenerTestsBase implements PulsarTestContainerSupport {

	@Autowired
	protected PulsarTemplate<String> pulsarTemplate;

	@Autowired
	protected PulsarClient pulsarClient;

	@Configuration(proxyBeanMethods = false)
	@EnableReactivePulsar
	public static class TopLevelConfig {

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

		@SuppressWarnings("unchecked")
		@Bean
		ConsumerTrackingReactivePulsarConsumerFactory<String> pulsarConsumerFactory(ReactivePulsarClient pulsarClient,
				ObjectProvider<ReactiveMessageConsumerBuilderCustomizer<String>> defaultConsumerCustomizersProvider) {
			DefaultReactivePulsarConsumerFactory<String> consumerFactory = new DefaultReactivePulsarConsumerFactory<>(
					pulsarClient, defaultConsumerCustomizersProvider.orderedStream().toList());
			return new ConsumerTrackingReactivePulsarConsumerFactory<>(consumerFactory);
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

		@Bean
		PulsarTopic partitionedTopic() {
			return PulsarTopic.builder("persistent://public/default/concurrency-on-pl").numberOfPartitions(3).build();
		}

		@Bean
		ReactivePulsarListenerMessageConsumerBuilderCustomizer<?> subscriptionInitialPositionEarliest() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

}

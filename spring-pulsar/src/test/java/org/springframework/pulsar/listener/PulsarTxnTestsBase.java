/*
 * Copyright 2023-2024 the original author or authors.
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

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.pulsar.transaction.PulsarAwareTransactionManager;
import org.springframework.pulsar.transaction.PulsarTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Provides base support for tests that use Pulsar transactions.
 *
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
@Testcontainers(disabledWithoutDocker = true)
class PulsarTxnTestsBase {

	static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage())
		.withTransactions();

	@BeforeAll
	static void startContainer() {
		PULSAR_CONTAINER.start();
	}

	@Autowired
	protected PulsarClient pulsarClient;

	@Autowired
	@Qualifier("transactionalPulsarTemplate")
	protected PulsarTemplate<String> transactionalPulsarTemplate;

	@Autowired
	@Qualifier("nonTransactionalPulsarTemplate")
	protected PulsarTemplate<String> nonTransactionalPulsarTemplate;

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	static class TopLevelConfig {

		@Bean
		PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarProducerFactory<>(pulsarClient, "foo-1");
		}

		@Bean
		PulsarClient pulsarClient() {
			return new DefaultPulsarClientFactory((clientBuilder) -> {
				clientBuilder.serviceUrl(PULSAR_CONTAINER.getPulsarBrokerUrl());
				clientBuilder.enableTransaction(true);
			}).createClient();
		}

		@Bean
		PulsarTemplate<String> transactionalPulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			template.transactions().setEnabled(true);
			return template;
		}

		@Bean
		PulsarTemplate<String> nonTransactionalPulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			var template = new PulsarTemplate<>(pulsarProducerFactory);
			template.transactions().setEnabled(false);
			return template;
		}

		@Bean
		public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient,
				ObjectProvider<ConsumerBuilderCustomizer<String>> defaultConsumerCustomizersProvider) {
			return new DefaultPulsarConsumerFactory<>(pulsarClient,
					defaultConsumerCustomizersProvider.orderedStream().toList());
		}

		@Bean
		PulsarListenerContainerFactory pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory,
				PulsarAwareTransactionManager pulsarTransactionManager) {
			var containerProps = new PulsarContainerProperties();
			containerProps.transactions().setEnabled(true);
			containerProps.transactions().setRequired(false);
			containerProps.transactions().setTransactionManager(pulsarTransactionManager);
			return new ConcurrentPulsarListenerContainerFactory<>(pulsarConsumerFactory, containerProps);
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PULSAR_CONTAINER.getHttpServiceUrl());
		}

		@Bean
		PulsarAwareTransactionManager pulsarTransactionManager(PulsarClient pulsarClient) {
			return new PulsarTransactionManager(pulsarClient);
		}

	}

}

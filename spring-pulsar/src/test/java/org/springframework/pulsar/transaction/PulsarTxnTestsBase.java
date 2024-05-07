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

package org.springframework.pulsar.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.test.support.PulsarConsumerTestUtil;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
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
public class PulsarTxnTestsBase {

	static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage())
		.withTransactions();

	@BeforeAll
	static void startPulsarContainer() {
		PULSAR_CONTAINER.start();
	}

	@Autowired
	protected PulsarClient pulsarClient;

	@Autowired
	protected PulsarTemplate<String> transactionalPulsarTemplate;

	protected PulsarTemplate<String> newNonTransactionalTemplate(boolean sendInBatch, int numMessages) {
		List<ProducerBuilderCustomizer<String>> customizers = List.of();
		if (sendInBatch) {
			customizers = List.of((pb) -> pb.enableBatching(true)
				.batchingMaxPublishDelay(2, TimeUnit.SECONDS)
				.batchingMaxMessages(numMessages));
		}
		return new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient, null, customizers));
	}

	protected void assertThatMessagesAreInTopic(String topicOut, String... expectedMessages) {
		assertMessagesInTopic(topicOut).contains(expectedMessages);
	}

	protected void assertThatMessagesAreNotInTopic(String topicOut, String... notExpectedMessages) {
		assertMessagesInTopic(topicOut).doesNotContain(notExpectedMessages);
	}

	protected AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>> assertMessagesInTopic(
			String topic) {
		return assertThat(PulsarConsumerTestUtil.<String>consumeMessages(pulsarClient)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.get()).map(Message::getValue);
	}

	@Configuration
	@EnablePulsar
	public static class TopLevelConfig {

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
		public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient,
				ObjectProvider<ConsumerBuilderCustomizer<String>> defaultConsumerCustomizersProvider) {
			return new DefaultPulsarConsumerFactory<>(pulsarClient,
					defaultConsumerCustomizersProvider.orderedStream().toList());
		}

		@Bean
		PulsarContainerProperties pulsarContainerProperties(PulsarAwareTransactionManager pulsarTransactionManager) {
			var containerProps = new PulsarContainerProperties();
			containerProps.transactions().setEnabled(true);
			containerProps.transactions().setRequired(false);
			containerProps.transactions().setTransactionManager(pulsarTransactionManager);
			return containerProps;
		}

		@Bean
		ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory, PulsarContainerProperties pulsarContainerProps) {
			return new ConcurrentPulsarListenerContainerFactory<>(pulsarConsumerFactory, pulsarContainerProps);
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

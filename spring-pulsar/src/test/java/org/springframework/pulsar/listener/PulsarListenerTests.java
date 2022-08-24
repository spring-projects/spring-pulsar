/*
 * Copyright 2022 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.core.AbstractContainerBaseTests;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Soby Chacko
 */
@SpringJUnitConfig
@DirtiesContext
public class PulsarListenerTests extends AbstractContainerBaseTests {

	static CountDownLatch latch = new CountDownLatch(1);

	@Autowired
	PulsarTemplate<String> pulsarTemplate;

	@Autowired
	private PulsarListenerEndpointRegistry registry;

	@Test
	void testPulsarListenerProvidedConsumerProperties() throws Exception {
		final PulsarContainerProperties pulsarContainerProperties = this.registry.getListenerContainer("foo")
				.getContainerProperties();
		final Properties pulsarConsumerProperties = pulsarContainerProperties.getPulsarConsumerProperties();
		assertThat(pulsarConsumerProperties.size()).isEqualTo(2);
		assertThat(pulsarConsumerProperties.get("topicNames")).isEqualTo("foo-1");
		assertThat(pulsarConsumerProperties.get("subscriptionName")).isEqualTo("subscription-1");
		pulsarTemplate.send("hello foo");
		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Configuration
	@EnablePulsar
	public static class Config {

		@PulsarListener(id = "foo", properties = { "subscriptionName=subscription-1", "topicNames=foo-1" })
		void listen1(String message) {
			latch.countDown();
		}

		@Bean
		public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			Map<String, Object> config = new HashMap<>();
			config.put("topicName", "foo-1");
			return new DefaultPulsarProducerFactory<>(pulsarClient, config);
		}

		@Bean
		public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
			return new PulsarClientFactoryBean(pulsarClientConfiguration);
		}

		@Bean
		public PulsarClientConfiguration pulsarClientConfiguration() {
			return new PulsarClientConfiguration(Map.of("serviceUrl", getPulsarBrokerUrl()));
		}

		@Bean
		public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			return new PulsarTemplate<>(pulsarProducerFactory);
		}

		@Bean
		public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient) {
			Map<String, Object> config = new HashMap<>();
			return new DefaultPulsarConsumerFactory<>(pulsarClient, config);
		}

		@Bean
		PulsarListenerContainerFactory<?> pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory) {
			final ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory = new ConcurrentPulsarListenerContainerFactory<>();
			pulsarListenerContainerFactory.setPulsarConsumerFactory(pulsarConsumerFactory);
			return pulsarListenerContainerFactory;
		}

	}

}

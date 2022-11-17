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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

/**
 * Tests for {@link PulsarListener}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
class PulsarListenerTests implements PulsarTestContainerSupport {

	private static final CountDownLatch LATCH_1 = new CountDownLatch(1);

	private static final CountDownLatch LATCH_2 = new CountDownLatch(10);

	@Test
	void basicPulsarListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("plt-topic1", "John Doe");
			assertThat(LATCH_1.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void batchPulsarListener() throws Exception {
		SpringApplication app = new SpringApplication(BatchListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("plt-topic2", "John Doe");
			}
			assertThat(LATCH_2.await(10, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@Import(PulsarAutoConfiguration.class)
	static class BasicListenerConfig {

		@PulsarListener(subscriptionName = "plt-subscription1", topics = "plt-topic1")
		public void listen(String foo) {
			LATCH_1.countDown();
		}

	}

	@Configuration(proxyBeanMethods = false)
	@Import(PulsarAutoConfiguration.class)
	static class BatchListenerConfig {

		@PulsarListener(subscriptionName = "plt-subscription2", topics = "plt-topic2", batch = true)
		public void listen(List<String> foo) {
			foo.forEach(t -> LATCH_2.countDown());
		}

	}

}

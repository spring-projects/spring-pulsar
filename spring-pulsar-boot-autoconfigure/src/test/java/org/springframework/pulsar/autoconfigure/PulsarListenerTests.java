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
class PulsarListenerTests extends AbstractContainerBaseTests {

	static CountDownLatch latch1 = new CountDownLatch(1);
	static CountDownLatch latch2 = new CountDownLatch(10);

	@Test
	void testBasicListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--spring.pulsar.client.serviceUrl=" + AbstractContainerBaseTests.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			final PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive");
			pulsarTemplate.send("John Doe");
			final boolean await = latch1.await(20, TimeUnit.SECONDS);
			assertThat(await).isTrue();
		}
	}

	@Test
	void testBatchListener() throws Exception {
		SpringApplication app = new SpringApplication(BatchListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--spring.pulsar.client.serviceUrl=" + AbstractContainerBaseTests.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			final PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive");
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("John Doe");
			}
			final boolean await = latch2.await(10, TimeUnit.SECONDS);
			assertThat(await).isTrue();
		}
	}

	@Configuration
	@Import(PulsarAutoConfiguration.class)
	public static class BasicListenerConfig {

		@PulsarListener(subscriptionName = "test-exclusive-sub-1", topics = "hello-pulsar-exclusive")
		public void listen(String foo) {
			latch1.countDown();
		}
	}

	@Configuration
	@Import(PulsarAutoConfiguration.class)
	public static class BatchListenerConfig {

		@PulsarListener(subscriptionName = "test-exclusive-sub-2", topics = "hello-pulsar-exclusive", batch = true)
		public void listen(List<String> foo) {
			foo.forEach(t -> latch2.countDown());
		}
	}
}

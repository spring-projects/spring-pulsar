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

package org.springframework.pulsar.core;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.pulsar.annotation.PulsarListener;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @author Soby Chacko
 */
public class PulsarListenerTests extends AbstractContainerBaseTest {

	static CountDownLatch latch1 = new CountDownLatch(1);
	static CountDownLatch latch2 = new CountDownLatch(10);

	@Test
	void testBasicListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--spring.pulsar.client.serviceUrl=" + getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			final PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive");
			pulsarTemplate.send("John Doe");
			System.out.println("waiting at the latch");
			final boolean await = latch1.await(20, TimeUnit.SECONDS);
			assertThat(await).isTrue();
		}
	}

	@Test
	void testBatchListener() throws Exception {
		SpringApplication app = new SpringApplication(BatchListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--spring.pulsar.client.serviceUrl=" + getPulsarBrokerUrl())) {
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

	@EnableAutoConfiguration
	public static class BasicListenerConfig {

		@PulsarListener(subscriptionName = "test-exclusive-sub-1", topics = "hello-pulsar-exclusive")
		public void listen(String foo) {
			System.out.println("Message Received from basic: " + foo);
			latch1.countDown();
		}
	}

	@EnableAutoConfiguration
	public static class BatchListenerConfig {

		@PulsarListener(subscriptionName = "test-exclusive-sub-2", topics = "hello-pulsar-exclusive", batch = "true")
		public void listen(List<String> foo) {
			System.out.println("Message Received from batch: " + foo);
			System.out.println("Message Received from batch: " + foo.size());
			foo.forEach(t -> latch2.countDown());
		}
	}
}

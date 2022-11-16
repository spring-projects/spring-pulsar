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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.pulsar.annotation.ReactivePulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.reactive.ReactiveMessageConsumerBuilderCustomizer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link ReactivePulsarListener}.
 *
 * @author Christophe Bornet
 */
class ReactivePulsarListenerTests implements PulsarTestContainerSupport {

	static CountDownLatch latch1 = new CountDownLatch(1);
	static CountDownLatch latch2 = new CountDownLatch(10);

	@Test
	void testBasicListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.setAllowCircularReferences(true);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			final PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("hello-pulsar-exclusive", "John Doe");
			final boolean await = latch1.await(20, TimeUnit.SECONDS);
			assertThat(await).isTrue();
		}
	}

	@Test
	void testFluxListener() throws Exception {
		SpringApplication app = new SpringApplication(FluxListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.setAllowCircularReferences(true);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			final PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("hello-pulsar-exclusive", "John Doe");
			}
			final boolean await = latch2.await(10, TimeUnit.SECONDS);
			assertThat(await).isTrue();
		}
	}

	@Configuration
	@Import({ PulsarAutoConfiguration.class, PulsarReactiveAutoConfiguration.class })
	static class BasicListenerConfig {

		@ReactivePulsarListener(subscriptionName = "test-exclusive-sub-1", topics = "hello-pulsar-exclusive",
				consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(String foo) {
			latch1.countDown();
			return Mono.empty();
		}

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

	@Configuration
	@Import({ PulsarAutoConfiguration.class, PulsarReactiveAutoConfiguration.class })
	static class FluxListenerConfig {

		@ReactivePulsarListener(subscriptionName = "test-exclusive-sub-2", topics = "hello-pulsar-exclusive",
				stream = true, consumerCustomizer = "consumerCustomizer")
		public Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
			return messages.doOnNext(t -> latch2.countDown()).map(m -> MessageResult.acknowledge(m.getMessageId()));
		}

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

}

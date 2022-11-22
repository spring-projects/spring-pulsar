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
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for {@link ReactivePulsarListener}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class ReactivePulsarListenerTests implements PulsarTestContainerSupport {

	private static final CountDownLatch LATCH1 = new CountDownLatch(1);

	private static final CountDownLatch LATCH2 = new CountDownLatch(10);

	@Test
	void basicListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("rplt-topic1", "John Doe");
			assertThat(LATCH1.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void fluxListener() throws Exception {
		SpringApplication app = new SpringApplication(FluxListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("rplt-topic2", "John Doe");
			}
			assertThat(LATCH2.await(10, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@Import({ PulsarAutoConfiguration.class, PulsarReactiveAutoConfiguration.class, ConsumerCustomizerConfig.class })
	static class BasicListenerConfig {

		@ReactivePulsarListener(subscriptionName = "rplt-subscription1", topics = "rplt-topic1",
				consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(String foo) {
			LATCH1.countDown();
			return Mono.empty();
		}

	}

	@Configuration(proxyBeanMethods = false)
	@Import({ PulsarAutoConfiguration.class, PulsarReactiveAutoConfiguration.class, ConsumerCustomizerConfig.class })
	static class FluxListenerConfig {

		@ReactivePulsarListener(subscriptionName = "rplt-subscription2", topics = "rplt-topic2", stream = true,
				consumerCustomizer = "consumerCustomizer")
		public Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
			return messages.doOnNext(t -> LATCH2.countDown()).map(MessageResult::acknowledge);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ConsumerCustomizerConfig {

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

}

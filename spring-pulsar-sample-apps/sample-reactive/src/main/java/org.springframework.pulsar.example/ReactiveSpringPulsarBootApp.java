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

package org.springframework.pulsar.example;

import java.time.Duration;
import java.util.Collections;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderTemplate;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class ReactiveSpringPulsarBootApp {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveSpringPulsarBootApp.class, args);
	}

	/**
	 * Sends POJO messages with a reactive template and receives them with a reactive
	 * consumer.
	 */
	@Configuration(proxyBeanMethods = false)
	static class ReactiveSenderWithReactiveConsumer implements ApplicationListener<ApplicationReadyEvent> {

		private final Logger logger = LoggerFactory.getLogger(this.getClass());

		@Autowired
		private ReactivePulsarSenderTemplate<Foo> reactivePulsarTemplate;

		// TODO remove this once the auto-config is available
		@Bean
		ReactivePulsarConsumerFactory<Foo> reactivePulsarConsumerFactory(ReactivePulsarClient reactivePulsarClient) {
			MutableReactiveMessageConsumerSpec spec = new MutableReactiveMessageConsumerSpec();
			spec.setTopicNames(Collections.singletonList("sample-reactive-topic1"));
			spec.setSubscriptionName("sample-reactive-sub1");
			spec.setConsumerName("sample-reactive-consumer1");
			return new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient, spec);
		}

		@Bean
		ApplicationRunner listenPojo(ReactivePulsarConsumerFactory<Foo> reactiveConsumerFactory) {
			return args -> {
				ReactiveMessageConsumer<Foo> messageConsumer = reactiveConsumerFactory
						.createConsumer(Schema.JSON(Foo.class));
				messageConsumer.consumeMany((messageFlux) -> messageFlux.map(MessageResult::acknowledgeAndReturn))
						.take(Duration.ofSeconds(10)).subscribe((msg) -> this.logger.info("Received: {}", msg));
			};
		}

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			this.reactivePulsarTemplate.setSchema(Schema.JSON(Foo.class));
			this.reactivePulsarTemplate
					.send("sample-reactive-topic1", Flux.range(0, 10).map((i) -> new Foo("Foo-" + i, "Bar-" + i)))
					.subscribe();
		}

	}

	/**
	 * Sends simple messages with a reactive template and receives them with an imperative
	 * listener.
	 */
	@Configuration(proxyBeanMethods = false)
	static class ReactiveSenderWithImperativeListener {

		private final Logger logger = LoggerFactory.getLogger(this.getClass());

		@Bean
		ApplicationRunner sendSimple(ReactivePulsarSenderTemplate<String> reactivePulsarTemplate) {
			return args -> reactivePulsarTemplate
					.send("sample-reactive-topic2", Flux.range(0, 10).map((i) -> "msg-from-sendSimple-" + i))
					.subscribe();
		}

		@PulsarListener(subscriptionName = "sample-reactive-sub2", topics = "sample-reactive-topic2")
		void listenSimple(String message) {
			this.logger.info("Received: {}", message);
		}

	}

	record Foo(String foo, String bar) {
	}

}

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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
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
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ReactiveSpringPulsarBootApp {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveSpringPulsarBootApp.class, args);
	}

	/**
	 * Sends string messages with a reactive template and receives them with a simple
	 * reactive listener.
	 */
	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithSimpleReactiveListener implements ApplicationListener<ApplicationReadyEvent> {

		private final Logger logger = LoggerFactory.getLogger(this.getClass());

		@Autowired
		private ReactivePulsarTemplate<String> reactivePulsarTemplate;

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			Flux.range(0, 10).map((i) -> "sample-message-" + i)
					.as(messages -> this.reactivePulsarTemplate.send("sample-reactive-topic1", messages)).subscribe();
		}

		@ReactivePulsarListener(subscriptionName = "sample-reactive-sub1", topics = "sample-reactive-topic1",
				consumerCustomizer = "subscriptionInitialPositionEarliest")
		public Mono<Void> listenSimple(String msg) {
			this.logger.info("Simple reactive listener received: {}", msg);
			return Mono.empty();
		}

	}

	/**
	 * Sends POJO messages with a reactive template and receives them with a reactive
	 * streaming listener.
	 */
	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithStreamingReactiveListener implements ApplicationListener<ApplicationReadyEvent> {

		private final Logger logger = LoggerFactory.getLogger(this.getClass());

		@Autowired
		private ReactivePulsarTemplate<Foo> reactivePulsarTemplate;

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			this.reactivePulsarTemplate.setSchema(Schema.JSON(Foo.class));
			Flux.range(0, 10).map((i) -> new Foo("Foo-" + i, "Bar-" + i))
					.as(messages -> this.reactivePulsarTemplate.send("sample-reactive-topic2", messages)).subscribe();
		}

		@ReactivePulsarListener(subscriptionName = "sample-reactive-sub2", topics = "sample-reactive-topic2",
				stream = true, schemaType = SchemaType.JSON, consumerCustomizer = "subscriptionInitialPositionEarliest")
		public Flux<MessageResult<Void>> listenStreaming(Flux<Message<Foo>> messages) {
			return messages
					.doOnNext((msg) -> this.logger.info("Streaming reactive listener received: {}", msg.getValue()))
					.map(m -> MessageResult.acknowledge(m.getMessageId()));
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ConsumerCustomizerConfig {

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<String> subscriptionInitialPositionEarliest() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

	/**
	 * Sends simple messages with a reactive template and receives them with an imperative
	 * listener.
	 */
	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithImperativeListener {

		private final Logger logger = LoggerFactory.getLogger(this.getClass());

		@Bean
		ApplicationRunner sendSimple(ReactivePulsarTemplate<String> reactivePulsarTemplate) {
			return args -> Flux.range(0, 10).map((i) -> "msg-from-sendSimple-" + i)
					.as(messages -> reactivePulsarTemplate.send("sample-reactive-topic3", messages)).subscribe();
		}

		@PulsarListener(subscriptionName = "sample-reactive-sub3", topics = "sample-reactive-topic3")
		void listenSimple(String message) {
			this.logger.info("Imperative listener received: {}", message);
		}

	}

	record Foo(String foo, String bar) {
	}

}

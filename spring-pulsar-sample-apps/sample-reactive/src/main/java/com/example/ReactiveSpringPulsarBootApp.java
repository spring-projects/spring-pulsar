/*
 * Copyright 2022-2024 the original author or authors.
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

package com.example;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ReactiveSpringPulsarBootApp {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveSpringPulsarBootApp.class);

	public static void main(String[] args) {
		SpringApplication.run(ReactiveSpringPulsarBootApp.class, args);
	}

	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithSimpleReactiveListener {

		private static final String TOPIC = "sample-reactive-topic1";

		@Bean
		ApplicationRunner sendPrimitiveMessagesToPulsarTopic(ReactivePulsarTemplate<String> template) {
			return (args) -> Flux.range(0, 10)
					.map((i) -> MessageSpec.of("ReactiveTemplateWithSimpleReactiveListener:" + i))
					.as(messages -> template.send(TOPIC, messages))
					.doOnNext((msr) -> LOG.info("++++++PRODUCE {}------", msr.getMessageSpec().getValue()))
					.subscribe();
		}

		@ReactivePulsarListener(topics = TOPIC, consumerCustomizer = "subscriptionInitialPositionEarliest")
		public Mono<Void> listenSimple(String msg) {
			LOG.info("++++++CONSUME {}------", msg);
			return Mono.empty();
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithStreamingReactiveListener {

		private static final String TOPIC = "sample-reactive-topic2";

		@Bean
		ApplicationRunner sendComplexMessagesToPulsarTopic(ReactivePulsarTemplate<Foo> template) {
			var schema = Schema.JSON(Foo.class);
			return (args) -> Flux.range(0, 10)
					.map((i) -> MessageSpec.of(new Foo("Foo-" + i, "Bar-" + i)))
					.as(messages -> template.send(TOPIC, messages, schema))
					.doOnNext((msr) -> LOG.info("++++++PRODUCE {}------", msr.getMessageSpec().getValue()))
					.subscribe();
		}

		@ReactivePulsarListener(topics = TOPIC, stream = true, schemaType = SchemaType.JSON,
				consumerCustomizer = "subscriptionInitialPositionEarliest")
		public Flux<MessageResult<Void>> listenStreaming(Flux<Message<Foo>> messages) {
			return messages
					.doOnNext((msg) -> LOG.info("++++++CONSUME {}------", msg.getValue()))
					.map(MessageResult::acknowledge);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ConsumerCustomizerConfig {

		@Bean
		ReactivePulsarListenerMessageConsumerBuilderCustomizer<String> subscriptionInitialPositionEarliest() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ReactiveTemplateWithImperativeListener {

		private static final String TOPIC = "sample-reactive-topic3";

		@Bean
		ApplicationRunner sendMessagesToPulsarTopic(ReactivePulsarTemplate<String> template) {
			return (args) -> Flux.range(0, 10)
					.map((i) -> MessageSpec.of("ReactiveTemplateWithImperativeListener:" + i))
					.as(messages -> template.send(TOPIC, messages))
					.doOnNext((msr) -> LOG.info("++++++PRODUCE {}------", msr.getMessageSpec().getValue()))
					.subscribe();
		}

		@PulsarListener(topics = TOPIC)
		void listenSimple(String msg) {
			LOG.info("++++++CONSUME {}------", msg);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeCustomObjectMapper {

		private static final String TOPIC = "sample-reactive-custom-object-mapper";

		@Bean
		SchemaResolver.SchemaResolverCustomizer<DefaultSchemaResolver> schemaResolverCustomizer() {
			return (DefaultSchemaResolver schemaResolver) -> {
				var objectMapper = UserRecordObjectMapper.withSerAndDeser();
				schemaResolver.setObjectMapper(objectMapper);
			};
		}

		@Bean
		ApplicationRunner sendWithCustomObjectMapper(ReactivePulsarTemplate<UserRecord> template) {
			return (args) -> Flux.range(0, 10)
					.map((i) -> MessageSpec.of(new UserRecord("user-" + i, 30)))
					.as(messages -> template.send(TOPIC, messages))
					.doOnNext((msr) -> LOG.info("++++++PRODUCE {}------", msr.getMessageSpec().getValue()))
					.subscribe();
		}

		@ReactivePulsarListener(topics = TOPIC, consumerCustomizer = "subscriptionInitialPositionEarliest")
		public Mono<Void> listenSimple(UserRecord user) {
			LOG.info("++++++CONSUME {}------", user);
			return Mono.empty();
		}

	}

	record Foo(String foo, String bar) {
	}

}

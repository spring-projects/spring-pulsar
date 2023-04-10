/*
 * Copyright 2022-2023 the original author or authors.
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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Integration tests for {@link ReactivePulsarListener}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 */
class ReactivePulsarListenerIntegrationTests implements PulsarTestContainerSupport {

	private static final CountDownLatch LATCH1 = new CountDownLatch(1);

	private static final CountDownLatch LATCH2 = new CountDownLatch(1);

	private static final CountDownLatch LATCH3 = new CountDownLatch(1);

	private static final CountDownLatch LATCH4 = new CountDownLatch(1);

	private static final CountDownLatch LATCH5 = new CountDownLatch(10);

	@Test
	void basicListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			ReactivePulsarTemplate<String> pulsarTemplate = context.getBean(ReactivePulsarTemplate.class);
			pulsarTemplate.send("rplt-topic1", "John Doe").block();
			assertThat(LATCH1.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicListenerCustomType() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerCustomTypeConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			ReactivePulsarTemplate<Foo> pulsarTemplate = context.getBean(ReactivePulsarTemplate.class);
			pulsarTemplate.send("rplt-custom-topic1", new Foo("John Doe"), Schema.JSON(Foo.class)).block();
			assertThat(LATCH2.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicListenerCustomTypeWithTypeMapping() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerCustomTypeWithTypeMappingConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			ReactivePulsarTemplate<Foo> pulsarTemplate = context.getBean(ReactivePulsarTemplate.class);
			pulsarTemplate.send("rplt-custom-topic2", new Foo("John Doe")).block();
			assertThat(LATCH3.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicPulsarListenerWithTopicMapping() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerWithTopicMappingConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
				.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			ReactivePulsarTemplate<Foo> pulsarTemplate = context.getBean(ReactivePulsarTemplate.class);
			pulsarTemplate.send("rplt-topicMapping-topic1", new Foo("Crazy8z"), Schema.JSON(Foo.class)).block();
			assertThat(LATCH4.await(20, TimeUnit.SECONDS)).isTrue();
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
				pulsarTemplate.send("rplt-batch-topic", "John Doe");
			}
			assertThat(LATCH5.await(10, TimeUnit.SECONDS)).isTrue();
		}
	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(ConsumerCustomizerConfig.class)
	static class BasicListenerConfig {

		@ReactivePulsarListener(subscriptionName = "rplt-sub1", topics = "rplt-topic1",
				consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(String ignored) {
			LATCH1.countDown();
			return Mono.empty();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(ConsumerCustomizerConfig.class)
	static class BasicListenerCustomTypeConfig {

		@ReactivePulsarListener(subscriptionName = "rplt-custom-sub1", topics = "rplt-custom-topic1",
				schemaType = SchemaType.JSON, consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(Foo ignored) {
			LATCH2.countDown();
			return Mono.empty();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(ConsumerCustomizerConfig.class)
	static class BasicListenerCustomTypeWithTypeMappingConfig {

		@Bean
		SchemaResolver customSchemaResolver() {
			DefaultSchemaResolver resolver = new DefaultSchemaResolver();
			resolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
			return resolver;
		}

		@ReactivePulsarListener(subscriptionName = "rplt-custom-sub2", topics = "rplt-custom-topic2",
				consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(Foo ignored) {
			LATCH3.countDown();
			return Mono.empty();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(ConsumerCustomizerConfig.class)
	static class BasicListenerWithTopicMappingConfig {

		@Bean
		TopicResolver customTopicResolver() {
			DefaultTopicResolver resolver = new DefaultTopicResolver();
			resolver.addCustomTopicMapping(Foo.class, "rplt-topicMapping-topic1");
			return resolver;
		}

		@ReactivePulsarListener(subscriptionName = "rplt-topicMapping-sub", schemaType = SchemaType.JSON,
				consumerCustomizer = "consumerCustomizer")
		public Mono<Void> listen(Foo ignored) {
			LATCH4.countDown();
			return Mono.empty();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(ConsumerCustomizerConfig.class)
	static class FluxListenerConfig {

		@ReactivePulsarListener(subscriptionName = "rplt-batch-sub", topics = "rplt-batch-topic", stream = true,
				consumerCustomizer = "consumerCustomizer")
		public Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
			return messages.doOnNext(t -> LATCH5.countDown()).map(MessageResult::acknowledge);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ConsumerCustomizerConfig {

		@Bean
		ReactiveMessageConsumerBuilderCustomizer<String> consumerCustomizer() {
			return b -> b.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		}

	}

	record Foo(String value) {
	}

}

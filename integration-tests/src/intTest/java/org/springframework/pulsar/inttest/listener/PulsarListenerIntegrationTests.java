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

package org.springframework.pulsar.inttest.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Integration tests for {@link PulsarListener}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
class PulsarListenerIntegrationTests implements PulsarTestContainerSupport {

	private static final CountDownLatch LATCH_1 = new CountDownLatch(1);

	private static final CountDownLatch LATCH_2 = new CountDownLatch(1);

	private static final CountDownLatch LATCH_3 = new CountDownLatch(1);

	private static final CountDownLatch LATCH_4 = new CountDownLatch(1);

	private static final CountDownLatch LATCH_5 = new CountDownLatch(10);

	@Test
	void basicPulsarListener() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
			.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("plit-basic-topic", "John Doe");
			assertThat(LATCH_1.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicPulsarListenerCustomType() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerCustomTypeConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
			.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<Foo> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("plit-foo-topic1", new Foo("John Doe"), Schema.JSON(Foo.class));
			assertThat(LATCH_2.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicPulsarListenerCustomTypeWithTypeMapping() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerCustomTypeWithTypeMappingConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
			.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<Foo> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("plit-foo-topic2", new Foo("John Doe"));
			assertThat(LATCH_3.await(20, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	void basicPulsarListenerWithTopicMapping() throws Exception {
		SpringApplication app = new SpringApplication(BasicListenerWithTopicMappingConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app
			.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl())) {
			@SuppressWarnings("unchecked")
			PulsarTemplate<Foo> pulsarTemplate = context.getBean(PulsarTemplate.class);
			pulsarTemplate.send("plit-topicMapping-topic", new Foo("Crazy8z"), Schema.JSON(Foo.class));
			assertThat(LATCH_4.await(20, TimeUnit.SECONDS)).isTrue();
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
				pulsarTemplate.send("plit-batch-topic", "John Doe");
			}
			assertThat(LATCH_5.await(10, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Nested
	class ConfigPropsDrivenListener {

		private static final CountDownLatch LATCH_CONFIG_PROPS = new CountDownLatch(1);

		@Test
		void subscriptionConfigPropsAreRespectedOnListener() throws Exception {
			SpringApplication app = new SpringApplication(ConfigPropsDrivenListenerConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext context = app.run(
					"--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--my.env=dev", "--spring.pulsar.consumer.topics=plit-config-props-topic-${my.env}",
					"--spring.pulsar.consumer.subscription.type=Shared",
					"--spring.pulsar.consumer.subscription.name=plit-config-props-subs-${my.env}")) {
				@SuppressWarnings("unchecked")
				PulsarTemplate<String> pulsarTemplate = context.getBean(PulsarTemplate.class);
				pulsarTemplate.send("plit-config-props-topic-dev", "hello config props driven");
				assertThat(LATCH_CONFIG_PROPS.await(10, TimeUnit.SECONDS)).isTrue();
			}

		}

		@EnableAutoConfiguration
		@SpringBootConfiguration
		static class ConfigPropsDrivenListenerConfig {

			@PulsarListener
			public void listen(String ignored, Consumer<String> consumer) {
				assertThat(consumer).extracting("conf", InstanceOfAssertFactories.type(ConsumerConfigurationData.class))
					.satisfies((conf) -> {
						assertThat(conf.getSingleTopic()).isEqualTo("persistent://public/default/plit-config-props-topic-dev");
						assertThat(conf.getSubscriptionType()).isEqualTo(SubscriptionType.Shared);
						assertThat(conf.getSubscriptionName()).isEqualTo("plit-config-props-subs-dev");
					});
				LATCH_CONFIG_PROPS.countDown();
			}

		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BasicListenerConfig {

		@PulsarListener(subscriptionName = "plit-basic-sub", topics = "plit-basic-topic")
		public void listen(String ignored) {
			LATCH_1.countDown();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BasicListenerCustomTypeConfig {

		@PulsarListener(subscriptionName = "plit-foo-sub1", topics = "plit-foo-topic1", schemaType = SchemaType.JSON)
		public void listen(Foo ignored) {
			LATCH_2.countDown();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BasicListenerCustomTypeWithTypeMappingConfig {

		@Bean
		SchemaResolver customSchemaResolver() {
			DefaultSchemaResolver resolver = new DefaultSchemaResolver();
			resolver.addCustomSchemaMapping(Foo.class, Schema.JSON(Foo.class));
			return resolver;
		}

		@PulsarListener(subscriptionName = "plit-foo-sub2", topics = "plit-foo-topic2")
		public void listen(Foo ignored) {
			LATCH_3.countDown();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BasicListenerWithTopicMappingConfig {

		@Bean
		TopicResolver customTopicResolver() {
			DefaultTopicResolver resolver = new DefaultTopicResolver();
			resolver.addCustomTopicMapping(Foo.class, "plit-topicMapping-topic");
			return resolver;
		}

		@PulsarListener(subscriptionName = "plit-topicMapping-sub", schemaType = SchemaType.JSON)
		public void listen(Foo ignored) {
			LATCH_4.countDown();
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BatchListenerConfig {

		@PulsarListener(subscriptionName = "plit-batch-sub", topics = "plit-batch-topic", batch = true)
		public void listen(List<String> foo) {
			foo.forEach(t -> LATCH_5.countDown());
		}

	}

	record Foo(String value) {
	}

}

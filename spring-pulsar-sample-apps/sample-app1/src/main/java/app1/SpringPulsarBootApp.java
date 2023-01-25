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

package app1;

import java.util.List;
import java.util.UUID;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;

@SpringBootApplication
public class SpringPulsarBootApp {

	private final Logger logger = LoggerFactory.getLogger(SpringPulsarBootApp.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarBootApp.class, args);
	}

	/*
	 * Basic publisher using PulsarTemplate<String> and a PulsarListener using an
	 * exclusive subscription to consume.
	 */
	@Bean
	ApplicationRunner runner1(PulsarTemplate<String> pulsarTemplate) {

		String topic1 = "hello-pulsar-exclusive-1";

		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send(topic1, "This is message " + (i + 1));
			}
		};
	}

	@PulsarListener(subscriptionName = "subscription-1", topics = "hello-pulsar-exclusive-1")
	void listen1(String message) {
		this.logger.info(message);
	}

	/*
	 * Basic publisher using PulsarTemplate<Integer> and a PulsarListener using an
	 * exclusive subscription to consume.
	 */
	@Bean
	ApplicationRunner runner2(PulsarTemplate<Integer> pulsarTemplate) {

		String topic1 = "hello-pulsar-exclusive-2";

		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send(topic1, i);
			}
		};
	}

	@PulsarListener(subscriptionName = "subscription-2", topics = "hello-pulsar-exclusive-2")
	void listen2(Integer message) {
		this.logger.info("Message received :" + message);
	}

	/*
	 * Demonstrating more complex types for publishing using JSON schema and the
	 * associated PulsarListener using an exclusive subscription.
	 */
	@Bean
	ApplicationRunner runner3(PulsarProducerFactory<Foo> producerFactory) {

		String topic = "hello-pulsar-exclusive-3";
		PulsarTemplate<Foo> pulsarTemplate = new PulsarTemplate<>(producerFactory);
		return args -> {
			for (int i = 0; i < 10; i++) {
				Foo foo = new Foo(i + "-" + "Foo-" + UUID.randomUUID(), i + "-" + "Bar-" + UUID.randomUUID());
				pulsarTemplate.send(topic, foo, Schema.JSON(Foo.class));
			}
		};
	}

	@PulsarListener(subscriptionName = "subscription-3", topics = "hello-pulsar-exclusive-3",
			schemaType = SchemaType.JSON)
	void listen3(Foo message) {
		this.logger.info("Message received :" + message);
	}

	/*
	 * Create a partitioned topic using PulsarAdministration and then publish to the topic
	 * and consume from it.
	 */
	@Bean
	PulsarTopic partitionedTopic4() {
		return PulsarTopic.builder("hello-pulsar-partitioned-4").numberOfPartitions(3).build();
	}

	@Bean
	ApplicationRunner runner4(PulsarTemplate<String> pulsarTemplate) {
		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send("hello-pulsar-partitioned-4", "This is message " + (i + 1));
			}
		};
	}

	@PulsarListener(subscriptionName = "subscription-4", topics = "hello-pulsar-partitioned-4")
	void listen4(String message) {
		this.logger.info("Message received from partitioned-topic : " + message);
	}

	/*
	 * Publish and then use PulsarListener in batch listening mode.
	 */
	@Bean
	ApplicationRunner runner5(PulsarProducerFactory<Foo> producerFactory) {

		String topic = "hello-pulsar-exclusive-5";
		PulsarTemplate<Foo> pulsarTemplate = new PulsarTemplate<>(producerFactory);
		return args -> {
			for (int i = 0; i < 100; i++) {
				Foo foo = new Foo(i + "-" + "Foo-" + UUID.randomUUID(), i + "-" + "Bar-" + UUID.randomUUID());
				pulsarTemplate.send(topic, foo, Schema.JSON(Foo.class));
			}
		};
	}

	@PulsarListener(subscriptionName = "subscription-5", topics = "hello-pulsar-exclusive-5",
			schemaType = SchemaType.JSON, batch = true)
	void listen5(List<Foo> messages) {
		this.logger.info("records received :" + messages.size());
		for (Foo message : messages) {
			this.logger.info("record : " + message);
		}
	}

	record Foo(String foo, String bar) {
		@Override
		public String toString() {
			return "Foo{" + "foo='" + this.foo + '\'' + ", bar='" + this.bar + '\'' + '}';
		}
	}

}

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

package com.example;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;

@SpringBootApplication
public class ImperativeProduceAndConsumeApp {

	private static final Logger LOG = LoggerFactory.getLogger(ImperativeProduceAndConsumeApp.class);

	public static void main(String[] args) {
		SpringApplication.run(ImperativeProduceAndConsumeApp.class, args);
	}

	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeWithPrimitiveMessageType {

		private static final String TOPIC = "produce-consume-primitive";

		@Bean
		ApplicationRunner sendPrimitiveMessagesToPulsarTopic(PulsarTemplate<String> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = "ProduceConsumeWithPrimitiveMessageType:" + i;
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub")
		void consumePrimitiveMessagesFromPulsarTopic(String msg) {
			LOG.info("++++++CONSUME {}------", msg);
		}

	}


	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeWithComplexMessageType {

		private static final String TOPIC = "produce-consume-complex";

		@Bean
		ApplicationRunner sendComplexMessagesToPulsarTopic(PulsarTemplate<Foo> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = new Foo("ProduceConsumeWithComplexMessageType", i);
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub")
		void consumeComplexMessagesFromPulsarTopic(Foo msg) {
			LOG.info("++++++CONSUME {}------", msg);
		}

	}


	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeWithPartitions {

		private static final String TOPIC = "produce-consume-partitions";

		@Bean
		PulsarTopic partitionedTopic() {
			return PulsarTopic.builder(TOPIC).numberOfPartitions(3).build();
		}

		@Bean
		ApplicationRunner sendPartitionedMessagesToPulsarTopic(PulsarTemplate<String> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = "ProduceConsumeWithPartitions:" + i;
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub")
		void consumePartitionedMessagesFromPulsarTopic(String msg) {
			LOG.info("++++++CONSUME {}------", msg);
		}

	}


	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeBatchListener {

		private static final String TOPIC = "produce-consume-batch";

		@Bean
		ApplicationRunner sendBatchMessagesToPulsarTopic(PulsarTemplate<Foo> template) {
			return (args) -> {
				for (int i = 0; i < 100; i++) {
					var msg = new Foo("ProduceConsumeBatchListener", i);
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub", batch = true)
		void consumeBatchMessagesFromPulsarTopic(List<Foo> messages) {
			messages.forEach((msg) -> LOG.info("++++++CONSUME {}------", msg));
		}

	}


	@Configuration(proxyBeanMethods = false)
	static class ProduceConsumeDefaultMappings {

		@Bean
		ApplicationRunner sendBarWithoutTopicOrSchema(PulsarTemplate<Bar> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = new Bar("ProduceConsumeDefaultMappings:" + i);
					// Default topic and schema mappings are in application.yml
					template.send(msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarListener
		void consumeBarWithoutTopicOrSchema(Bar msg) {
			LOG.info("++++++CONSUME {}------", msg);
		}

	}

	record Foo(String name, Integer value) {
	}

	public record Bar(String value) {
	}
}

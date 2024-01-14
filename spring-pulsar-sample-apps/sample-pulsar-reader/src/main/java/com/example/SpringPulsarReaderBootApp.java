/*
 * Copyright 2023-2024 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class SpringPulsarReaderBootApp {

	private static final Logger LOG = LoggerFactory.getLogger(SpringPulsarReaderBootApp.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarReaderBootApp.class, args);
	}

	@Configuration(proxyBeanMethods = false)
	static class ProduceAndReadWithPrimitiveMessageType {

		private static final String TOPIC = "produce-read-primitive";

		@Bean
		ApplicationRunner sendPrimitiveMessagesToPulsarTopic(PulsarTemplate<String> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = "ProduceAndReadWithPrimitiveMessageType:" + i;
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarReader(topics = TOPIC, startMessageId = "earliest")
		void readPrimitiveMessagesFromPulsarTopic(String msg) {
			LOG.info("++++++READ {}------", msg);
		}

	}

	@Configuration(proxyBeanMethods = false)
	static class ProduceAndReadWithComplexMessageType {

		private static final String TOPIC = "produce-read-complex";

		@Bean
		ApplicationRunner sendComplexMessagesToPulsarTopic(PulsarTemplate<Foo> template) {
			return (args) -> {
				for (int i = 0; i < 10; i++) {
					var msg = new Foo("ProduceAndReadWithComplexMessageType", i);
					template.send(TOPIC, msg);
					LOG.info("++++++PRODUCE {}------", msg);
				}
			};
		}

		@PulsarReader(topics = TOPIC, startMessageId = "earliest")
		void readComplexMessagesFromPulsarTopic(Foo msg) {
			LOG.info("++++++READ {}------", msg);
		}

	}

	record Foo(String name, Integer value) {
	}

}

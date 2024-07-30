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

package org.springframework.pulsar.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.reader.PulsarReaderHeaderTests.WithCustomObjectMapperTest.WithCustomObjectMapperTestConfig;
import org.springframework.pulsar.reader.PulsarReaderHeaderTests.WithStandardObjectMapperTest.WithStandardObjectMapperTestConfig;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.model.json.UserRecordDeserializer;
import org.springframework.test.context.ContextConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Tests consuming records with header in {@link PulsarReader @PulsarReader}.
 *
 * @author Chris Bono
 */
class PulsarReaderHeaderTests extends PulsarReaderTestsBase {

	@Nested
	@ContextConfiguration(classes = WithStandardObjectMapperTestConfig.class)
	class WithStandardObjectMapperTest {

		private static final String TOPIC = "prht-with-standard-mapper-topic";

		private static CountDownLatch listenerLatch = new CountDownLatch(1);

		private static UserRecord userPassedIntoListener;

		@Test
		void whenObjectMapperIsNotDefinedThenStandardMapperUsedToDeserHeaders() throws Exception {
			var msg = "hello-%s".formatted(TOPIC);
			// In order to send complex headers (pojo) must manually map and set each
			// header as follows
			var user = new UserRecord("that", 100);
			var headers = new HashMap<String, Object>();
			headers.put("user", user);
			var headerMapper = JsonPulsarHeaderMapper.builder().build();
			var mappedHeaders = headerMapper.toPulsarHeaders(new MessageHeaders(headers));
			pulsarTemplate.newMessage(msg)
				.withMessageCustomizer(messageBuilder -> mappedHeaders.forEach(messageBuilder::property))
				.withTopic(TOPIC)
				.send();
			assertThat(listenerLatch.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(userPassedIntoListener).isEqualTo(user);
		}

		@Configuration(proxyBeanMethods = false)
		static class WithStandardObjectMapperTestConfig {

			@PulsarReader(topics = TOPIC, startMessageId = "earliest")
			public void listenWithHeaders(org.apache.pulsar.client.api.Message<String> msg,
					@Header("user") UserRecord user) {
				userPassedIntoListener = user;
				listenerLatch.countDown();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = WithCustomObjectMapperTestConfig.class)
	class WithCustomObjectMapperTest {

		private static final String TOPIC = "prht-with-custom-mapper-topic";

		private static CountDownLatch listenerLatch = new CountDownLatch(1);

		private static UserRecord userPassedIntoListener;

		@Test
		void whenObjectMapperIsDefinedThenItIsUsedToDeserHeaders() throws Exception {
			var msg = "hello-%s".formatted(TOPIC);
			// In order to send complex headers (pojo) must manually map and set each
			// header as follows
			var user = new UserRecord("that", 100);
			var headers = new HashMap<String, Object>();
			headers.put("user", user);
			var headerMapper = JsonPulsarHeaderMapper.builder().build();
			var mappedHeaders = headerMapper.toPulsarHeaders(new MessageHeaders(headers));
			MessageId messageId = pulsarTemplate.newMessage(msg)
				.withMessageCustomizer(messageBuilder -> mappedHeaders.forEach(messageBuilder::property))
				.withTopic(TOPIC)
				.send();
			// Custom deser adds suffix to name and bumps age + 5
			var expectedUser = new UserRecord(user.name() + "-deser", user.age() + 5);
			assertThat(listenerLatch.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(userPassedIntoListener).isEqualTo(expectedUser);
		}

		@Configuration(proxyBeanMethods = false)
		static class WithCustomObjectMapperTestConfig {

			@Bean(name = "pulsarHeaderObjectMapper")
			ObjectMapper customObjectMapper() {
				var objectMapper = new ObjectMapper();
				var module = new SimpleModule();
				module.addDeserializer(UserRecord.class, new UserRecordDeserializer());
				objectMapper.registerModule(module);
				return objectMapper;
			}

			@PulsarReader(topics = TOPIC, startMessageId = "earliest")
			public void listenWithHeaders(org.apache.pulsar.client.api.Message<String> msg,
					@Header("user") UserRecord user) {
				userPassedIntoListener = user;
				listenerLatch.countDown();
			}

		}

	}

}

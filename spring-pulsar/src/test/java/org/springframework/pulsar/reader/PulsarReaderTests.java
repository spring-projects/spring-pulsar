/*
 * Copyright 2023 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.config.DefaultPulsarReaderContainerFactory;
import org.springframework.pulsar.config.PulsarReaderContainerFactory;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarReaderFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * {@link PulsarReader} integration tests.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
public class PulsarReaderTests implements PulsarTestContainerSupport {

	@Autowired
	PulsarTemplate<String> pulsarTemplate;

	@Autowired
	private PulsarClient pulsarClient;

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	public static class TopLevelConfig {

		@Bean
		public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarProducerFactory<>(pulsarClient);
		}

		@Bean
		public PulsarClient pulsarClient() throws PulsarClientException {
			return new DefaultPulsarClientFactory(PulsarTestContainerSupport.getPulsarBrokerUrl()).createClient();
		}

		@Bean
		public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
			return new PulsarTemplate<>(pulsarProducerFactory);
		}

		@Bean
		public PulsarReaderFactory<?> pulsarReaderFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarReaderFactory<>(pulsarClient);
		}

		@Bean
		PulsarReaderContainerFactory pulsarReaderContainerFactory(PulsarReaderFactory<Object> pulsarReaderFactory) {
			return new DefaultPulsarReaderContainerFactory<>(pulsarReaderFactory,
					new PulsarReaderContainerProperties());
		}

	}

	@Nested
	@ContextConfiguration(classes = StartMessageIdEarliest.PulsarReaderStartMessageIdEarliest.class)
	class StartMessageIdEarliest {

		static CountDownLatch latch = new CountDownLatch(1);

		@Test
		void startMessageIdEarliest() throws Exception {
			pulsarTemplate.send("pulsarReaderBasicScenario-topic-1", "hello foo");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class PulsarReaderStartMessageIdEarliest {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-1",
					subscriptionName = "pulsarReaderBasicScenario-subscription-1",
					topics = "pulsarReaderBasicScenario-topic-1", startMessageId = "earliest")
			void read(String ignored) {
				latch.countDown();
			}

		}

	}

	@Nested
	class StartMessageIdMissing {

		@Test
		void startMessageIdMissing() {
			assertThatThrownBy(() -> new AnnotationConfigApplicationContext(TopLevelConfig.class,
					PulsarReaderStartMessageIdMissing.class)).rootCause().isInstanceOf(IllegalArgumentException.class)
							.hasMessage(
									"Start message id or start message from roll back must be specified but they cannot be specified at the same time");
		}

		@EnablePulsar
		static class PulsarReaderStartMessageIdMissing {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-2",
					subscriptionName = "pulsarReaderBasicScenario-subscription-2",
					topics = "pulsarReaderBasicScenario-topic-2")
			void readWithoutStartMessageId(String ignored) {

			}

		}

	}

	@Nested
	class StartMessageIdLatest {

		static CountDownLatch latch = new CountDownLatch(1);

		@Test
		void startMessageIdLatest() throws Exception {
			pulsarTemplate.send("pulsarReaderBasicScenario-topic-3", "hello foo");
			new AnnotationConfigApplicationContext(TopLevelConfig.class,
					StartMessageIdLatest.PulsarReaderStartMessageIdLatest.class);
			pulsarTemplate.send("pulsarReaderBasicScenario-topic-3", "hello foobar");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class PulsarReaderStartMessageIdLatest {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-3",
					subscriptionName = "pulsarReaderBasicScenario-subscription-3",
					topics = "pulsarReaderBasicScenario-topic-3", startMessageId = "latest")
			void read(String msg) {
				latch.countDown();
				assertThat(msg).isEqualTo("hello foobar");
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = StartMessageIdFromTheMiddleOfTheTopic.WithCustomizerConfig.class)
	class StartMessageIdFromTheMiddleOfTheTopic {

		private static final CountDownLatch latch = new CountDownLatch(5);

		@Test
		void startMessageIdProvidedThroughReaderCustomizer() throws Exception {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}

		@EnablePulsar
		@Configuration
		static class WithCustomizerConfig {

			int currentIndex = 5;

			MessageId[] messageIds = new MessageId[10];

			@PulsarReader(id = "with-customizer-reader", subscriptionName = "with-customizer-reader-subscription",
					topics = "with-customizer-reader-topic", readerCustomizer = "myCustomizer")
			void listen(Message<String> message) {
				assertThat(message.getMessageId()).isEqualTo(messageIds[currentIndex++]);
				latch.countDown();
			}

			@Bean
			public ReaderBuilderCustomizer<String> myCustomizer(PulsarTemplate<String> pulsarTemplate) {
				return cb -> {
					for (int i = 0; i < 10; i++) {
						try {
							messageIds[i] = pulsarTemplate.send("with-customizer-reader-topic", "hello john doe-");
						}
						catch (PulsarClientException e) {
							// Ignore
						}
					}
					cb.startMessageId(messageIds[4]); // the first message read is the one
														// after this message id.
				};
			}

		}

	}

}

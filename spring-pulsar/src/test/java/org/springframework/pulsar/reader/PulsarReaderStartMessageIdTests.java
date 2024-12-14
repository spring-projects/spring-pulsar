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

package org.springframework.pulsar.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.annotation.PulsarReaderReaderBuilderCustomizer;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for {@link PulsarReader}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Jonas Geiregat
 */
public class PulsarReaderStartMessageIdTests extends PulsarReaderTestsBase {

	@Nested
	@ContextConfiguration(classes = StartMessageIdEarliest.PulsarReaderStartMessageIdEarliest.class)
	class StartMessageIdEarliest {

		static CountDownLatch latch = new CountDownLatch(1);

		@Test
		void startMessageIdEarliest() throws Exception {
			pulsarTemplate.send("pulsarReaderBasicScenario-topic-1", "hello foo");
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class PulsarReaderStartMessageIdEarliest {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-1", topics = "pulsarReaderBasicScenario-topic-1",
					startMessageId = "earliest")
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
					PulsarReaderStartMessageIdMissing.class))
				.rootCause()
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageStartingWith(
						"Start message id or start message from roll back must be specified but they cannot be specified at the same time");
		}

		@EnablePulsar
		static class PulsarReaderStartMessageIdMissing {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-2", topics = "pulsarReaderBasicScenario-topic-2")
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

		@Configuration(proxyBeanMethods = false)
		static class PulsarReaderStartMessageIdLatest {

			@PulsarReader(id = "pulsarReaderBasicScenario-id-3", topics = "pulsarReaderBasicScenario-topic-3",
					startMessageId = "latest")
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
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}

		@Configuration(proxyBeanMethods = false)
		static class WithCustomizerConfig {

			int currentIndex = 5;

			MessageId[] messageIds = new MessageId[10];

			@PulsarReader(id = "with-customizer-reader", topics = "with-customizer-reader-topic",
					readerCustomizer = "myCustomizer")
			void listen(Message<String> message) {
				assertThat(message.getMessageId()).isEqualTo(messageIds[currentIndex++]);
				latch.countDown();
			}

			@Bean
			public PulsarReaderReaderBuilderCustomizer<String> myCustomizer(PulsarTemplate<String> pulsarTemplate) {
				return cb -> {
					for (int i = 0; i < 10; i++) {
						messageIds[i] = pulsarTemplate.send("with-customizer-reader-topic", "hello john doe-");
					}
					// the first message read is the one after this message id
					cb.startMessageId(messageIds[4]);

				};
			}

		}

	}

}

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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Testing {@link DefaultPulsarReaderFactory}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarReaderFactoryTests implements PulsarTestContainerSupport {

	private PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		this.pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (this.pulsarClient != null && !this.pulsarClient.isClosed()) {
			this.pulsarClient.close();
		}
	}

	@Nested
	class BasicScenarioTests {

		private PulsarReaderFactory<String> pulsarReaderFactory;

		@BeforeEach
		void createReaderFactory() {
			pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient);
		}

		@Test
		void readingFromTheBeginningOfTheTopic() throws Exception {
			Message<String> message;
			try (Reader<String> reader = pulsarReaderFactory.createReader(List.of("basic-pulsar-reader-topic"),
					MessageId.earliest, Schema.STRING, Collections.emptyList())) {

				PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
						"basic-pulsar-reader-topic");
				PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
				pulsarTemplate.send("hello john doe");

				message = reader.readNext();
			}
			assertThat(message.getValue()).isEqualTo("hello john doe");
		}

		@Test
		void readingFromTheMiddleOfTheTopic() throws Exception {
			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					"reading-from-the-middle-of-topic");
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			MessageId[] messageIds = new MessageId[10];

			for (int i = 0; i < 10; i++) {
				messageIds[i] = pulsarTemplate.send("hello john doe-" + i);
			}

			Message<String> message;
			try (Reader<String> reader = pulsarReaderFactory.createReader(List.of("reading-from-the-middle-of-topic"),
					messageIds[4], Schema.STRING, Collections.emptyList())) {
				for (int i = 0; i < 5; i++) {
					message = reader.readNext();
					assertThat(message.getValue()).isEqualTo("hello john doe-" + (i + 5));
				}
				message = reader.readNext(1, TimeUnit.SECONDS);
				assertThat(message).isNull();
			}
		}

		@Test
		void readingFromTheEndOfTheTopic() throws Exception {
			Message<String> message;

			PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
					"basic-pulsar-reader-topic");
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			pulsarTemplate.send("hello john doe");

			try (Reader<String> reader = pulsarReaderFactory.createReader(List.of("basic-pulsar-reader-topic"),
					MessageId.latest, Schema.STRING, Collections.emptyList())) {
				pulsarTemplate.send("hello alice doe");
				// It should not read the first message sent (john doe) as latest is the
				// message id to start.
				message = reader.readNext();
				assertThat(message.getValue()).isEqualTo("hello alice doe");
				message = reader.readNext(1, TimeUnit.SECONDS);
				assertThat(message).isNull();
			}
		}

		@Test
		void useFactoryDefaults() throws Exception {
			pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient, List.of((readerBuilder) -> {
				readerBuilder.topic("basic-pulsar-reader-topic");
				readerBuilder.startMessageId(MessageId.earliest);
			}));
			// The following code expects the above topic and startMessageId to be used
			Message<String> message;
			try (Reader<String> reader = pulsarReaderFactory.createReader(null, null, Schema.STRING,
					Collections.emptyList())) {
				PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
						"basic-pulsar-reader-topic");
				PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
				pulsarTemplate.send("hello john doe");
				message = reader.readNext();
			}
			assertThat(message.getValue()).isEqualTo("hello john doe");
		}

		@Test
		void overrideFactoryDefaults() throws Exception {
			pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient, List.of((readerBuilder) -> {
				readerBuilder.topic("foo-topic");
				readerBuilder.startMessageId(MessageId.latest);
			}));
			// The following code expects the above topic and startMessageId to be ignored
			// (overridden)
			Message<String> message;
			try (Reader<String> reader = pulsarReaderFactory.createReader(List.of("basic-pulsar-reader-topic"),
					MessageId.earliest, Schema.STRING, Collections.emptyList())) {

				PulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
						"basic-pulsar-reader-topic");
				PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
				pulsarTemplate.send("hello john doe");

				message = reader.readNext();
			}
			assertThat(message.getValue()).isEqualTo("hello john doe");
		}

		@Test
		void customizersAreAppliedLast() throws Exception {
			ReaderBuilderCustomizer<String> customizer = (readerBuilder) -> readerBuilder
				.topic("basic-pulsar-reader-topic");
			// The following code expects the above topic will override the passed in
			// 'foo-topic'
			try (var reader = pulsarReaderFactory.createReader(List.of("foo-topic"), MessageId.earliest, Schema.STRING,
					List.of(customizer))) {
				var pulsarProducerFactory = new DefaultPulsarProducerFactory<String>(pulsarClient,
						"basic-pulsar-reader-topic");
				var pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
				pulsarTemplate.send("hello john doe");
				assertThat(reader.readNext().getValue()).isEqualTo("hello john doe");
			}
		}

	}

	@Nested
	@SuppressWarnings("unchecked")
	class DefaultConfigCustomizerApi {

		private ReaderBuilderCustomizer<String> configCustomizer1 = mock(ReaderBuilderCustomizer.class);

		private ReaderBuilderCustomizer<String> configCustomizer2 = mock(ReaderBuilderCustomizer.class);

		private ReaderBuilderCustomizer<String> createReaderCustomizer = mock(ReaderBuilderCustomizer.class);

		@Test
		void singleConfigCustomizer() throws Exception {
			try (var ignored = new DefaultPulsarReaderFactory<>(pulsarClient,
					List.of(configCustomizer2, configCustomizer1))
				.createReader(List.of("basic-pulsar-reader-topic"), MessageId.earliest, Schema.STRING,
						List.of(createReaderCustomizer))) {
				InOrder inOrder = inOrder(configCustomizer1, createReaderCustomizer);
				inOrder.verify(configCustomizer1).customize(any(ReaderBuilder.class));
				inOrder.verify(createReaderCustomizer).customize(any(ReaderBuilder.class));
			}
		}

		@Test
		void multipleConfigCustomizers() throws Exception {
			try (var ignored = new DefaultPulsarReaderFactory<>(pulsarClient, List.of(configCustomizer1)).createReader(
					List.of("basic-pulsar-reader-topic"), MessageId.earliest, Schema.STRING,
					List.of(createReaderCustomizer))) {
				InOrder inOrder = inOrder(configCustomizer1, configCustomizer2, createReaderCustomizer);
				inOrder.verify(configCustomizer2).customize(any(ReaderBuilder.class));
				inOrder.verify(configCustomizer1).customize(any(ReaderBuilder.class));
				inOrder.verify(createReaderCustomizer).customize(any(ReaderBuilder.class));
			}
		}

	}

	@Nested
	class MissingConfig {

		private PulsarReaderFactory<String> pulsarReaderFactory;

		@BeforeEach
		void createReaderFactory() {
			pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient);
		}

		@Test
		void missingTopic() {
			// topic name is not set in the API call or in the reader config.
			assertThatThrownBy(() -> pulsarReaderFactory.createReader(Collections.emptyList(), MessageId.earliest,
					Schema.STRING, Collections.emptyList()))
				.isInstanceOf(PulsarClientException.class)
				.hasMessageContaining("Topic name must be set on the reader builder");
		}

		@Test
		void missingStartingMessageId() {
			assertThatThrownBy(() -> pulsarReaderFactory.createReader(List.of("my-reader-topic"), null, Schema.STRING,
					Collections.emptyList()))
				.isInstanceOf(PulsarClientException.class)
				.hasMessageContaining(
						"Start message id or start message from roll back must be specified but they cannot be specified at the same time");
		}

	}

}

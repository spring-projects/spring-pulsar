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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarReaderFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Basic tests for {@link DefaultPulsarReaderListenerContainer}.
 *
 * @author Soby Chacko
 */
public class DefaultPulsarReaderListenerContainerTests implements PulsarTestContainerSupport {

	@Nullable
	private PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (pulsarClient != null && !pulsarClient.isClosed()) {
			pulsarClient.close();
		}
	}

	@Test
	void basicDefaultReader() throws Exception {
		Map<String, Object> config = Map.of("topicNames", Collections.singleton("dprlct-001"), "subscriptionName",
				"dprlct-sub-001");

		DefaultPulsarReaderFactory<String> pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient, config);
		CountDownLatch latch = new CountDownLatch(1);
		PulsarReaderContainerProperties readerContainerProperties = new PulsarReaderContainerProperties();
		readerContainerProperties.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("hello john doe");
			latch.countDown();
		});
		readerContainerProperties.setStartMessageId(MessageId.earliest);
		readerContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarReaderListenerContainer<String> container = new DefaultPulsarReaderListenerContainer<>(
				pulsarReaderFactory, readerContainerProperties);
		container.start();

		Map<String, Object> prodConfig = Map.of("topicName", "dprlct-001");
		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				prodConfig);
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		pulsarTemplate.sendAsync("hello john doe");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	void topicProvidedThroughContainerProperties() throws Exception {
		Map<String, Object> config = Collections.emptyMap();

		DefaultPulsarReaderFactory<String> pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient, config);
		CountDownLatch latch = new CountDownLatch(1);
		PulsarReaderContainerProperties readerContainerProperties = new PulsarReaderContainerProperties();
		readerContainerProperties.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("hello buzz doe");
			latch.countDown();
		});
		readerContainerProperties.setStartMessageId(MessageId.earliest);
		readerContainerProperties.setTopics(List.of("dprlct-002"));
		readerContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarReaderListenerContainer<String> container = new DefaultPulsarReaderListenerContainer<>(
				pulsarReaderFactory, readerContainerProperties);
		container.start();

		Map<String, Object> prodConfig = Map.of("topicName", "dprlct-002");
		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				prodConfig);
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		pulsarTemplate.sendAsync("hello buzz doe");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	void latestMessageId() throws Exception {
		Map<String, Object> config = Collections.emptyMap();

		DefaultPulsarReaderFactory<String> pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient, config);
		CountDownLatch latch = new CountDownLatch(2);
		PulsarReaderContainerProperties readerContainerProperties = new PulsarReaderContainerProperties();
		readerContainerProperties.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("This message should be received by the reader");
			latch.countDown();
		});
		readerContainerProperties.setStartMessageId(MessageId.latest);
		readerContainerProperties.setTopics(List.of("dprlct-003"));
		readerContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarReaderListenerContainer<String> container = new DefaultPulsarReaderListenerContainer<>(
				pulsarReaderFactory, readerContainerProperties);

		Map<String, Object> prodConfig = Map.of("topicName", "dprlct-003");
		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				prodConfig);
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);

		// The following sends will not be received by the reader as we are using the
		// latest message id to start from.
		for (int i = 0; i < 5; i++) {
			pulsarTemplate.sendAsync("This message should not be received by the reader");
		}

		container.start();

		pulsarTemplate.sendAsync("This message should be received by the reader");
		pulsarTemplate.sendAsync("This message should be received by the reader");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

}

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

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarReaderFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Basic tests for {@link DefaultPulsarMessageReaderContainer}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarMessageReaderContainerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

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
		var latch = new CountDownLatch(1);

		DefaultPulsarReaderFactory<String> pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient,
				(readerBuilder -> {
					readerBuilder.topic("dprlct-001");
					readerBuilder.subscriptionName("dprlct-sub-001");
				}));
		var readerContainerProperties = new PulsarReaderContainerProperties();
		readerContainerProperties.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("hello john doe");
			latch.countDown();
		});
		readerContainerProperties.setStartMessageId(MessageId.earliest);
		readerContainerProperties.setSchema(Schema.STRING);

		DefaultPulsarMessageReaderContainer<String> container = null;
		try {
			container = new DefaultPulsarMessageReaderContainer<>(pulsarReaderFactory, readerContainerProperties);
			container.start();

			DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, "dprlct-001", List.of((pb) -> pb.topic("dprlct-001")));
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			pulsarTemplate.sendAsync("hello john doe");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
		}
	}

	@Test
	void topicProvidedThroughContainerProperties() throws Exception {
		var latch = new CountDownLatch(1);
		var containerProps = new PulsarReaderContainerProperties();

		DefaultPulsarReaderFactory<String> pulsarReaderFactory = new DefaultPulsarReaderFactory<>(pulsarClient);
		containerProps.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("hello buzz doe");
			latch.countDown();
		});
		containerProps.setStartMessageId(MessageId.earliest);
		containerProps.setTopics(List.of("dprlct-002"));
		containerProps.setSchema(Schema.STRING);
		DefaultPulsarMessageReaderContainer<String> container = null;
		try {
			container = new DefaultPulsarMessageReaderContainer<>(pulsarReaderFactory, containerProps);
			container.start();
			DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(
					pulsarClient, "dprlct-002", List.of((pb) -> pb.topic("dprlct-002")));
			PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
			pulsarTemplate.sendAsync("hello buzz doe");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
		}
	}

	@Test
	void latestMessageId() throws Exception {
		var latch = new CountDownLatch(2);
		var containerProps = new PulsarReaderContainerProperties();
		containerProps.setReaderListener((ReaderListener<?>) (reader, msg) -> {
			assertThat(msg.getValue()).isEqualTo("This message should be received by the reader");
			latch.countDown();
		});
		containerProps.setStartMessageId(MessageId.latest);
		containerProps.setTopics(List.of("dprlct-003"));
		containerProps.setSchema(Schema.STRING);

		var readerFactory = new DefaultPulsarReaderFactory<String>(pulsarClient);
		DefaultPulsarMessageReaderContainer<String> container = null;
		try {
			container = new DefaultPulsarMessageReaderContainer<>(readerFactory, containerProps);

			var prodConfig = Map.<String, Object>of("topicName", "dprlct-003");
			var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, "dprlct-003",
					List.of((pb) -> pb.topic("dprlct-003")));
			var pulsarTemplate = new PulsarTemplate<>(producerFactory);

			// The following sends will not be received by the reader as we are using the
			// latest message id to start from.
			for (int i = 0; i < 5; i++) {
				pulsarTemplate.send("This message should not be received by the reader");
			}
			container.start();
			assertThat(container.isRunning()).isTrue();
			pulsarTemplate.sendAsync("This message should be received by the reader");
			pulsarTemplate.sendAsync("This message should be received by the reader");

			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			safeStopContainer(container);
		}
	}

	private void safeStopContainer(PulsarMessageReaderContainer container) {
		try {
			container.stop();
		}
		catch (Exception ex) {
			logger.warn(ex, "Failed to stop container %s: %s".formatted(container, ex.getMessage()));
		}
	}

}

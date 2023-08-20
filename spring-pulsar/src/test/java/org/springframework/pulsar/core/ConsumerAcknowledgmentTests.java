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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarAcknowledgingMessageListener;
import org.springframework.pulsar.listener.PulsarBatchMessageListener;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarRecordMessageListener;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * @author Soby Chacko
 */
class ConsumerAcknowledgmentTests implements PulsarTestContainerSupport {

	@Test
	void testRecordAck() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-011", "cons-ack-tests-sb-011")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setAckMode(AckMode.RECORD);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		CountDownLatch latch = new CountDownLatch(10);

		doAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).when(containerConsumer).acknowledge(any(MessageId.class));

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-011");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		container.stop();
		pulsarClient.close();
	}

	@Test
	void testBatchAck() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-012", "cons-ack-tests-sb-012")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> latch.countDown());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-012");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, never()).acknowledge(any(Message.class)));
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, atLeastOnce()).acknowledgeCumulative(any(Message.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	void testBatchAckButSomeRecordsFail() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-013", "cons-ack-tests-sb-013")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		CountDownLatch latch = new CountDownLatch(10);
		pulsarContainerProperties.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> {
			latch.countDown();
			if (latch.getCount() % 2 == 0) {
				throw new RuntimeException("fail");
			}
		});
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		AtomicInteger ackCallCount = new AtomicInteger(0);
		doAnswer(invocation -> {
			ackCallCount.incrementAndGet();
			return invocation.callRealMethod();
		}).when(containerConsumer).acknowledge(any(MessageId.class));

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-013");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(1_000);
		// Half of the message get acknowledged, and the other half gets negatively
		// acknowledged.

		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, times(5)).negativeAcknowledge(any(Message.class)));

		int ackCalls = ackCallCount.get();
		if (ackCalls < 5) {
			await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> verify(containerConsumer, atMost(4)).acknowledge(any(MessageId.class)));
			await().atMost(Duration.ofSeconds(10))
				.untilAsserted(
						() -> verify(containerConsumer, atLeastOnce()).acknowledgeCumulative(any(Message.class)));
			if (ackCalls == 0) {
				await().atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> verify(containerConsumer, times(5)).acknowledgeCumulative(any(Message.class)));
			}
		}
		else {
			assertThat(ackCalls == 5).isTrue();
		}
		container.stop();
		pulsarClient.close();
	}
	// CHECKSTYLE:ON

	@Test
	@SuppressWarnings("unchecked")
	void testManualAckForRecordListener() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-014", "cons-ack-tests-sb-014")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		List<Acknowledgement> acksObjects = new ArrayList<>();
		PulsarAcknowledgingMessageListener<?> pulsarAcknowledgingMessageListener = (consumer, msg, acknowledgement) -> {
			acksObjects.add(acknowledgement);
			acknowledgement.acknowledge();
		};
		pulsarContainerProperties.setMessageListener(pulsarAcknowledgingMessageListener);
		pulsarContainerProperties.setSchema(Schema.STRING);
		pulsarContainerProperties.setAckMode(AckMode.MANUAL);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		CountDownLatch latch = new CountDownLatch(10);

		doAnswer(invocation -> {
			latch.countDown();
			return invocation.callRealMethod();
		}).when(containerConsumer).acknowledge(any(MessageId.class));

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-014");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		// We are asserting that we got 10 valid ack objects through the receive method
		// invocation.
		assertThat(acksObjects.size()).isEqualTo(10);
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, times(10)).acknowledge(any(MessageId.class)));

		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBatchAckForBatchListener() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-015", "cons-ack-tests-sb-015")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMaxNumMessages(10);
		pulsarContainerProperties.setBatchTimeoutMillis(60_000);
		pulsarContainerProperties.setBatchListener(true);
		CountDownLatch latch = new CountDownLatch(1);
		PulsarBatchMessageListener<?> pulsarBatchMessageListener = mock(PulsarBatchMessageListener.class);
		doAnswer(invocation -> {
			latch.countDown();
			return null;
		}).when(pulsarBatchMessageListener).received(any(Consumer.class), any(List.class));
		pulsarContainerProperties.setMessageListener(pulsarBatchMessageListener);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-015");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(
					() -> verify(pulsarBatchMessageListener, times(1)).received(any(Consumer.class), any(List.class)));
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, times(1)).acknowledgeCumulative(any(Message.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBatchNackForEntireBatchWhenUsingBatchListener() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = spy(new DefaultPulsarConsumerFactory<>(
				pulsarClient, defaultConfig("cons-ack-tests-016", "cons-ack-tests-sb-016")));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		pulsarContainerProperties.setMaxNumMessages(10);
		pulsarContainerProperties.setBatchTimeoutMillis(60_000);
		pulsarContainerProperties.setBatchListener(true);
		PulsarBatchMessageListener<?> pulsarBatchMessageListener = mock(PulsarBatchMessageListener.class);
		CountDownLatch latch = new CountDownLatch(1);
		doAnswer(invocation -> {
			latch.countDown();
			throw new RuntimeException();
		}).when(pulsarBatchMessageListener).received(any(Consumer.class), any(List.class));
		pulsarContainerProperties.setMessageListener(pulsarBatchMessageListener);
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		Consumer<String> containerConsumer = ConsumerTestUtils.startContainerAndSpyOnConsumer(container);

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"cons-ack-tests-016");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		for (int i = 0; i < 10; i++) {
			pulsarTemplate.sendAsync("hello john doe");
		}
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(2_000);
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, times(1)).negativeAcknowledge(any(Messages.class)));
		await().atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> verify(containerConsumer, never()).acknowledge(any(Messages.class)));
		container.stop();
		pulsarClient.close();
	}

	@Test
	void messagesAreProperlyAckdOnContainerStopBeforeExitingListenerThread() throws Exception {
		PulsarClient pulsarClient = PulsarClient.builder()
			.serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.build();
		DefaultPulsarConsumerFactory<String> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient,
				defaultConfig("duplicate-message-test", "duplicate-sub-1"));
		PulsarContainerProperties pulsarContainerProperties = new PulsarContainerProperties();
		AtomicInteger counter1 = new AtomicInteger(0);
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> counter1.getAndIncrement());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container1 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container1.start();

		DefaultPulsarProducerFactory<String> pulsarProducerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				"duplicate-message-test");
		PulsarTemplate<String> pulsarTemplate = new PulsarTemplate<>(pulsarProducerFactory);
		pulsarTemplate.send("hello john doe");

		while (counter1.get() == 0) {
			// busy wait until counter1 is > 0
		}
		// When we stop, if any acks are in progress, that should all be
		// taken care of before exiting the listener thread, so that the
		// next consumer under the same subscription will not receive the
		// unacked message.
		container1.stop();

		AtomicInteger counter2 = new AtomicInteger(0);
		pulsarContainerProperties
			.setMessageListener((PulsarRecordMessageListener<?>) (consumer, msg) -> counter2.getAndIncrement());
		pulsarContainerProperties.setSchema(Schema.STRING);
		DefaultPulsarMessageListenerContainer<String> container2 = new DefaultPulsarMessageListenerContainer<>(
				pulsarConsumerFactory, pulsarContainerProperties);
		container2.start();

		pulsarTemplate.send("hello john doe");

		while (counter2.get() == 0) {
			// busy wait until counter2 > 0
		}
		// Asserting that both consumers are only receiving the expected data.
		assertThat(counter1.get()).isEqualTo(1);
		assertThat(counter2.get()).isEqualTo(1);

		container2.stop();
		pulsarClient.close();
	}

	private <T> List<ConsumerBuilderCustomizer<T>> defaultConfig(String topicName, String subscriptionName) {
		return List.of((consumerBuilder) -> {
			consumerBuilder.topic(topicName);
			consumerBuilder.subscriptionName(subscriptionName);
		});
	}

}

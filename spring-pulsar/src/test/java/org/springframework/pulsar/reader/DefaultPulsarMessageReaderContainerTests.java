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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarReaderFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.event.ReaderFailedToStartEvent;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

/**
 * Basic tests for {@link DefaultPulsarMessageReaderContainer}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarMessageReaderContainerTests implements PulsarTestContainerSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private @Nullable PulsarClient pulsarClient;

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
				List.of((readerBuilder) -> {
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
		var container = new DefaultPulsarMessageReaderContainer<>(pulsarReaderFactory, containerProps);
		try {
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
			var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, "dprlct-003",
					List.of((pb) -> pb.topic("dprlct-003")));
			var pulsarTemplate = new PulsarTemplate<>(producerFactory);

			// The following sends will not be received by the reader as we are using the
			// latest message id to start from.
			for (int i = 0; i < 5; i++) {
				pulsarTemplate.send("This message should not be received by the reader");
			}
			container.start();
			await().atMost(Duration.ofSeconds(10)).until(container::isRunning);
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

	@SuppressWarnings("unchecked")
	@Nested
	class WithStartupFailures {

		@Test
		void whenPolicyIsStopThenExceptionIsThrown() throws Exception {
			DefaultPulsarReaderFactory<String> readerFactory = mock(DefaultPulsarReaderFactory.class);
			var containerProps = new PulsarReaderContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.STOP);
			containerProps.setSchema(Schema.STRING);
			containerProps.setReaderListener((ReaderListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageReaderContainer<>(readerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex when create reader
			var failCause = new PulsarException("please-stop");
			when(readerFactory.createReader(any(), any(), any(), any())).thenThrow(failCause);
			// start container and expect ex thrown
			assertThatIllegalStateException().isThrownBy(() -> container.start())
				.withMessageStartingWith("Error starting reader container")
				.withCause(failCause);
			assertThat(container.isRunning()).isFalse();
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ReaderFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsContinueThenExceptionIsNotThrown() throws Exception {
			DefaultPulsarReaderFactory<String> readerFactory = mock(DefaultPulsarReaderFactory.class);
			var containerProps = new PulsarReaderContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.CONTINUE);
			containerProps.setSchema(Schema.STRING);
			containerProps.setReaderListener((ReaderListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageReaderContainer<>(readerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex when create reader
			var failCause = new PulsarException("please-continue");
			when(readerFactory.createReader(any(), any(), any(), any())).thenThrow(failCause);
			// start container and expect ex not thrown
			container.start();
			assertThat(container.isRunning()).isFalse();
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ReaderFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsRetryAndRetriesAreExhaustedThenContainerDoesNotStart() throws Exception {
			DefaultPulsarReaderFactory<String> readerFactory = mock(DefaultPulsarReaderFactory.class);
			var retryCount = new AtomicInteger(0);
			var thrown = new ArrayList<Throwable>();
			var retryListener = new RetryListener() {
				@Override
				public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					retryCount.set(context.getRetryCount());
				}

				@Override
				public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					thrown.add(throwable);
				}
			};
			var retryTemplate = RetryTemplate.builder()
				.maxAttempts(2)
				.fixedBackoff(Duration.ofSeconds(2))
				.withListener(retryListener)
				.build();
			var containerProps = new PulsarReaderContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			containerProps.setSchema(Schema.STRING);
			containerProps.setReaderListener((ReaderListener<?>) (__, ___) -> {
			});
			var container = new DefaultPulsarMessageReaderContainer<>(readerFactory, containerProps);
			var eventPublisher = mock(ApplicationEventPublisher.class);
			container.setApplicationEventPublisher(eventPublisher);
			// setup factory to throw ex on 3 attempts (initial + 2 retries)
			var failCause = new PulsarException("please-retry-exhausted");
			doThrow(failCause).doThrow(failCause)
				.doThrow(failCause)
				.when(readerFactory)
				.createReader(any(), any(), any(), any());
			container.start();

			// start container and expect ex not thrown and 2 retries
			await().atMost(Duration.ofSeconds(15)).until(() -> retryCount.get() == 2);
			assertThat(thrown).containsExactly(failCause, failCause);
			assertThat(container.isRunning()).isFalse();
			// factory called 3x (initial + 2 retries)
			verify(readerFactory, times(3)).createReader(any(), any(), any(), any());
			verify(eventPublisher)
				.publishEvent(assertArg((evt) -> assertThat(evt).isInstanceOf(ReaderFailedToStartEvent.class)
					.hasFieldOrPropertyWithValue("container", container)));
		}

		@Test
		void whenPolicyIsRetryAndRetryIsSuccessfulThenContainerStarts() throws Exception {
			var topic = "dprlct-wsf-retry";
			var readerFactory = spy(new DefaultPulsarReaderFactory<String>(pulsarClient, List.of((readerBuilder) -> {
				readerBuilder.topic(topic);
				readerBuilder.subscriptionName(topic + "-sub");
				readerBuilder.startMessageId(MessageId.earliest);
			})));
			var retryCount = new AtomicInteger(0);
			var thrown = new ArrayList<Throwable>();
			var retryListener = new RetryListener() {
				@Override
				public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					retryCount.set(context.getRetryCount());
				}

				@Override
				public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
						Throwable throwable) {
					thrown.add(throwable);
				}
			};
			var retryTemplate = RetryTemplate.builder()
				.maxAttempts(3)
				.fixedBackoff(Duration.ofSeconds(2))
				.withListener(retryListener)
				.build();
			var latch = new CountDownLatch(1);
			var containerProps = new PulsarReaderContainerProperties();
			containerProps.setStartupFailurePolicy(StartupFailurePolicy.RETRY);
			containerProps.setStartupFailureRetryTemplate(retryTemplate);
			containerProps.setReaderListener((ReaderListener<?>) (reader, msg) -> latch.countDown());
			containerProps.setSchema(Schema.STRING);
			var container = new DefaultPulsarMessageReaderContainer<>(readerFactory, containerProps);
			try {
				var eventPublisher = mock(ApplicationEventPublisher.class);
				container.setApplicationEventPublisher(eventPublisher);
				// setup factory to throw ex on initial call and 1st retry - then succeed
				// on 2nd retry
				var failCause = new PulsarException("please-retry");
				doThrow(failCause).doThrow(failCause)
					.doCallRealMethod()
					.when(readerFactory)
					.createReader(any(), any(), any(), any());
				// start container and expect started after retries
				container.start();
				await().atMost(Duration.ofSeconds(20)).until(container::isRunning);

				// factory called 3x (initial call + 2 retries)
				verify(readerFactory, times(3)).createReader(any(), any(), any(), any());
				// only had to retry once (2nd call in retry template succeeded)
				assertThat(retryCount).hasValue(1);
				assertThat(thrown).containsExactly(failCause);
				// should be able to process messages
				var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, topic);
				var pulsarTemplate = new PulsarTemplate<>(producerFactory);
				pulsarTemplate.sendAsync("hello-" + topic);
				assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			}
			finally {
				safeStopContainer(container);
			}
		}

	}

}

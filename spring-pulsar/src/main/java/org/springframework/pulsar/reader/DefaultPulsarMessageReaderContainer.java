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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.pulsar.event.ReaderFailedToStartEvent;
import org.springframework.pulsar.event.ReaderStartedEvent;
import org.springframework.pulsar.event.ReaderStartingEvent;
import org.springframework.scheduling.SchedulingAwareRunnable;

/**
 * Default implementation for the Pulsar reader container.
 *
 * This implementation is responsible for all the lifecycle management for the reader
 * container.
 *
 * @param <T> reader data type.
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarMessageReaderContainer<T> extends AbstractPulsarMessageReaderContainer<T> {

	private final AtomicReference<InternalAsyncReader> internalAsyncReader = new AtomicReference<>();

	private volatile CompletableFuture<?> readerFuture;

	private final AbstractPulsarMessageReaderContainer<?> thisOrParentContainer;

	private final AtomicReference<Thread> readerThread = new AtomicReference<>();

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	public DefaultPulsarMessageReaderContainer(PulsarReaderFactory<? super T> pulsarReaderFactory,
			PulsarReaderContainerProperties pulsarReaderContainerProperties) {
		super(pulsarReaderFactory, pulsarReaderContainerProperties);
		this.thisOrParentContainer = this;
	}

	@Override
	protected void doStart() {
		var containerProperties = getContainerProperties();
		var readerExecutor = containerProperties.getReaderTaskExecutor();
		if (readerExecutor == null) {
			readerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setReaderTaskExecutor(readerExecutor);
		}
		@SuppressWarnings("unchecked")
		var readerListener = (ReaderListener<T>) containerProperties.getReaderListener();
		try {
			this.internalAsyncReader.set(new InternalAsyncReader(readerListener, containerProperties));
		}
		catch (Exception e) {
			var msg = "Error starting reader container [%s]".formatted(this.getBeanName());
			this.logger.error(e, () -> msg);
			if (containerProperties.getStartupFailurePolicy() != StartupFailurePolicy.RETRY) {
				this.publishReaderFailedToStart();
			}
			if (containerProperties.getStartupFailurePolicy() == StartupFailurePolicy.STOP) {
				this.logger.info(() -> "Configured to stop on startup failures - exiting");
				throw new IllegalStateException(msg, e);
			}
		}

		if (this.internalAsyncReader.get() != null) {
			this.logger.debug(() -> "Successfully created completable - submitting to executor");
			this.readerFuture = readerExecutor.submitCompletable(this.internalAsyncReader.get());
			waitForStartup(containerProperties.getReaderStartTimeout());
		}
		else if (containerProperties.getStartupFailurePolicy() == StartupFailurePolicy.RETRY) {
			this.logger.info(() -> "Configured to retry on startup failures - retrying asynchronously");
			this.readerFuture = readerExecutor.submitCompletable(() -> {
				var retryTemplate = Optional.ofNullable(containerProperties.getStartupFailureRetryTemplate())
					.orElseGet(containerProperties::getDefaultStartupFailureRetryTemplate);
				this.internalAsyncReader.set(retryTemplate.<InternalAsyncReader, PulsarException>execute(
						(__) -> new InternalAsyncReader(readerListener, containerProperties)));
				this.internalAsyncReader.get().run();
			}).whenComplete((__, ex) -> {
				if (ex == null) {
					this.logger
						.info(() -> "Successfully re-started reader container [%s]".formatted(this.getBeanName()));
				}
				else {
					this.logger.error(ex, () -> "Unable to re-start reader container [%s] - retries exhausted"
						.formatted(this.getBeanName()));
					this.publishReaderFailedToStart();
				}
			});
		}
	}

	private void waitForStartup(Duration waitTime) {
		try {
			if (!this.startLatch.await(waitTime.toMillis(), TimeUnit.MILLISECONDS)) {
				this.logger.error("Consumer thread failed to start - does the configured task executor "
						+ "have enough threads to support all containers and concurrency?");
				publishReaderFailedToStart();
			}
		}
		catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doStop() {
		setRunning(false);
		try {
			this.logger.info("Closing this consumer.");
			var asyncReaderRef = this.internalAsyncReader.get();
			if (asyncReaderRef != null && asyncReaderRef.reader != null) {
				asyncReaderRef.reader.close();
			}
		}
		catch (IOException e) {
			this.logger.error(e, () -> "Error closing Pulsar Client.");
		}
	}

	private void publishReaderStartingEvent() {
		this.setRunning(true);
		this.startLatch.countDown();
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ReaderStartingEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishReaderStartedEvent() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ReaderStartedEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishReaderFailedToStart() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ReaderFailedToStartEvent(this, this.thisOrParentContainer));
		}
	}

	private final class InternalAsyncReader implements SchedulingAwareRunnable {

		private final ReaderListener<T> listener;

		private final PulsarReaderContainerProperties readerContainerProperties;

		private Reader<T> reader;

		private final ReaderBuilderCustomizer<T> readerBuilderCustomizer;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		InternalAsyncReader(ReaderListener<T> readerListener,
				PulsarReaderContainerProperties readerContainerProperties) {
			this.listener = readerListener;
			this.readerContainerProperties = readerContainerProperties;
			this.readerBuilderCustomizer = getReaderBuilderCustomizer();
			List<ReaderBuilderCustomizer<T>> customizers = this.readerBuilderCustomizer != null
					? List.of(this.readerBuilderCustomizer) : Collections.emptyList();
			this.reader = getPulsarReaderFactory().createReader(readerContainerProperties.getTopics(),
					readerContainerProperties.getStartMessageId(), (Schema) readerContainerProperties.getSchema(),
					customizers);
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			DefaultPulsarMessageReaderContainer.this.readerThread.set(Thread.currentThread());
			publishReaderStartingEvent();
			publishReaderStartedEvent();
			while (isRunning()) {
				try {
					Message<T> message = this.reader.readNext();
					this.listener.received(this.reader, message);
				}
				catch (PulsarClientException e) {
					DefaultPulsarMessageReaderContainer.this.logger.error(e, () -> "Error receiving messages.");
				}
			}
		}

	}

}

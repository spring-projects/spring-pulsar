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

import java.io.IOException;
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
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.pulsar.core.PulsarReaderFactory;
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
 */
public class DefaultPulsarReaderListenerContainer<T> extends AbstractPulsarReaderListenerContainer<T> {

	private final AtomicReference<InternalAsyncReader> internalAsyncReader = new AtomicReference<>();

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	private volatile CompletableFuture<?> readerFuture;

	private final AbstractPulsarReaderListenerContainer<?> thisOrParentContainer;

	private final AtomicReference<Thread> readerThread = new AtomicReference<>();

	public DefaultPulsarReaderListenerContainer(PulsarReaderFactory<? super T> pulsarReaderFactory,
			PulsarReaderContainerProperties pulsarReaderContainerProperties) {
		super(pulsarReaderFactory, pulsarReaderContainerProperties);
		this.thisOrParentContainer = this;
	}

	@Override
	protected void doStart() {
		PulsarReaderContainerProperties containerProperties = getContainerProperties();

		Object readerListenerObject = containerProperties.getReaderListener();
		AsyncTaskExecutor readerExecutor = containerProperties.getReaderTaskExecutor();

		@SuppressWarnings("unchecked")
		ReaderListener<T> readerListener = (ReaderListener<T>) readerListenerObject;

		if (readerExecutor == null) {
			readerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setReaderTaskExecutor(readerExecutor);
		}

		this.internalAsyncReader.set(new InternalAsyncReader(readerListener, containerProperties));

		setRunning(true);
		this.startLatch = new CountDownLatch(1);
		this.readerFuture = readerExecutor.submitCompletable(this.internalAsyncReader.get());

		try {
			if (!this.startLatch.await(containerProperties.getReaderStartTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
				this.logger.error("Reader thread failed to start - does the configured task executor "
						+ "have enough threads to support all containers and concurrency?");
				publishReaderFailedToStart();
			}
		}
		catch (@SuppressWarnings("UNUSED") InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doStop() {
		setRunning(false);
		try {
			this.logger.info("Closing this consumer.");
			this.internalAsyncReader.get().reader.close();
		}
		catch (IOException e) {
			this.logger.error(e, () -> "Error closing Pulsar Client.");
		}
	}

	private void publishReaderStartingEvent() {
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

		@SuppressWarnings({ "unchecked", "rawtypes" })
		InternalAsyncReader(ReaderListener<T> readerListener,
				PulsarReaderContainerProperties readerContainerProperties) {
			this.listener = readerListener;
			this.readerContainerProperties = readerContainerProperties;

			try {
				this.reader = getPulsarReaderFactory().createReader(readerContainerProperties.getTopics(),
						readerContainerProperties.getStartMessageId(), (Schema) readerContainerProperties.getSchema());
			}
			catch (PulsarClientException e) {
				DefaultPulsarReaderListenerContainer.this.logger.error(e, () -> "Pulsar client exceptions.");
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			DefaultPulsarReaderListenerContainer.this.readerThread.set(Thread.currentThread());
			publishReaderStartingEvent();
			publishReaderStartedEvent();

			while (isRunning()) {
				try {
					Message<T> message = this.reader.readNext();
					this.listener.received(this.reader, message);
				}
				catch (PulsarClientException e) {
					DefaultPulsarReaderListenerContainer.this.logger.error(e, () -> "Error receiving messages.");
				}
			}
		}

	}

}

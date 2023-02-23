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

import org.apache.pulsar.client.api.ReaderListener;

import org.springframework.pulsar.core.AbstractPulsarMessageContainer;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.util.Assert;

/**
 * Core implementation for {@link PulsarMessageReaderContainer}.
 *
 * @param <T> reader data type.
 * @author Soby Chacko
 */
public non-sealed abstract class AbstractPulsarMessageReaderContainer<T> extends AbstractPulsarMessageContainer
		implements PulsarMessageReaderContainer {

	private final PulsarReaderFactory<T> pulsarReaderFactory;

	private final PulsarReaderContainerProperties pulsarReaderContainerProperties;

	protected final Object lifecycleMonitor = new Object();

	protected ReaderBuilderCustomizer<T> readerBuilderCustomizer;

	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageReaderContainer(PulsarReaderFactory<? super T> pulsarReaderFactory,
			PulsarReaderContainerProperties pulsarReaderContainerProperties) {
		this.pulsarReaderFactory = (PulsarReaderFactory<T>) pulsarReaderFactory;
		this.pulsarReaderContainerProperties = pulsarReaderContainerProperties;
	}

	public PulsarReaderFactory<T> getPulsarReaderFactory() {
		return this.pulsarReaderFactory;
	}

	public PulsarReaderContainerProperties getContainerProperties() {
		return this.pulsarReaderContainerProperties;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public void setupReaderListener(Object messageListener) {
		this.pulsarReaderContainerProperties.setReaderListener(messageListener);
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Assert.state(this.pulsarReaderContainerProperties.getReaderListener() instanceof ReaderListener<?>,
						() -> "A " + ReaderListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
	}

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				doStop();
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setReaderCustomizer(ReaderBuilderCustomizer<?> readerBuilderCustomizer) {
		this.readerBuilderCustomizer = (ReaderBuilderCustomizer<T>) readerBuilderCustomizer;
	}

	public ReaderBuilderCustomizer<T> getReaderBuilderCustomizer() {
		return this.readerBuilderCustomizer;
	}

}

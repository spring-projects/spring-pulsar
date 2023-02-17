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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.util.Assert;

/**
 * Core implementation for {@link PulsarReaderListenerContainer}.
 *
 * @param <T> reader data type.
 * @author Soby Chacko
 */
public non-sealed abstract class AbstractPulsarReaderListenerContainer<T> implements PulsarReaderListenerContainer,
		BeanNameAware, ApplicationEventPublisherAware, ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarReaderFactory<T> pulsarReaderFactory;

	private final PulsarReaderContainerProperties pulsarReaderContainerProperties;

	private ApplicationEventPublisher applicationEventPublisher;

	private String beanName;

	private ApplicationContext applicationContext;

	private boolean autoStartup = true;

	private int phase;

	protected final Object lifecycleMonitor = new Object();

	private volatile boolean running = false;

	@SuppressWarnings("unchecked")
	protected AbstractPulsarReaderListenerContainer(PulsarReaderFactory<? super T> pulsarReaderFactory,
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
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	/**
	 * Get the event publisher.
	 * @return the publisher
	 */
	@Nullable
	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Nullable
	public String getBeanName() {
		return this.beanName; // the container factory sets this to the listener id
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Nullable
	protected ApplicationContext getApplicationContext() {
		return this.applicationContext;
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

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	protected abstract void doStart();

	protected abstract void doStop();

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

}

/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.pulsar.listener;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.RedeliveryBackoff;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.util.Assert;

/**
 * Base implementation for the {@link PulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Soby Chacko
 */
public abstract class AbstractPulsarMessageListenerContainer<T> implements PulsarMessageListenerContainer,
		BeanNameAware, ApplicationEventPublisherAware, ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); // NOSONAR

	private ApplicationEventPublisher applicationEventPublisher;

	private String beanName;

	private ApplicationContext applicationContext;

	private final PulsarContainerProperties pulsarContainerProperties;

	protected final PulsarConsumerFactory<T> pulsarConsumerFactory;

	private boolean autoStartup = true;

	private int phase;

	protected final Object lifecycleMonitor = new Object();

	private volatile boolean running = false;

	protected RedeliveryBackoff negativeAckRedeliveryBackoff;

	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		this.pulsarContainerProperties = pulsarContainerProperties;
		this.pulsarConsumerFactory = (PulsarConsumerFactory<T>) pulsarConsumerFactory;

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

	/**
	 * Return the bean name.
	 * @return the bean name.
	 */
	@Nullable
	public String getBeanName() {
		return this.beanName;
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
	public void setupMessageListener(Object messageListener) {
		this.pulsarContainerProperties.setMessageListener(messageListener);
	}

	public PulsarContainerProperties getPulsarContainerProperties() {
		return this.pulsarContainerProperties;
	}

	public PulsarConsumerFactory<? super T> getPulsarConsumerFactory() {
		return this.pulsarConsumerFactory;
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

	public PulsarContainerProperties getContainerProperties() {
		return this.pulsarContainerProperties;
	}

	protected abstract void doStart();

	protected abstract void doStop();

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Assert.state(this.pulsarContainerProperties.getMessageListener() instanceof PulsarRecordMessageListener,
						() -> "A " + PulsarRecordMessageListener.class.getName() + " implementation must be provided");
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

	@Override
	public void setNegativeAckRedeliveryBackoff(RedeliveryBackoff redeliveryBackoff) {
		this.negativeAckRedeliveryBackoff = redeliveryBackoff;
	}

	public RedeliveryBackoff getNegativeAckRedeliveryBackoff() {
		return this.negativeAckRedeliveryBackoff;
	}

}

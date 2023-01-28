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

package org.springframework.pulsar.listener;

import org.apache.pulsar.client.api.DeadLetterPolicy;
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

import io.micrometer.observation.ObservationRegistry;

/**
 * Base implementation for the {@link PulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public non-sealed abstract class AbstractPulsarMessageListenerContainer<T> implements PulsarMessageListenerContainer,
		BeanNameAware, ApplicationEventPublisherAware, ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarConsumerFactory<T> pulsarConsumerFactory;

	private final PulsarContainerProperties pulsarContainerProperties;

	private final ObservationRegistry observationRegistry;

	private ApplicationEventPublisher applicationEventPublisher;

	private String beanName;

	private ApplicationContext applicationContext;

	private boolean autoStartup = true;

	private int phase;

	protected final Object lifecycleMonitor = new Object();

	private volatile boolean running = false;

	private volatile boolean paused;

	protected RedeliveryBackoff negativeAckRedeliveryBackoff;

	protected RedeliveryBackoff ackTimeoutRedeliveryBackoff;

	protected DeadLetterPolicy deadLetterPolicy;

	protected PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties, @Nullable ObservationRegistry observationRegistry) {
		this.pulsarConsumerFactory = (PulsarConsumerFactory<T>) pulsarConsumerFactory;
		this.pulsarContainerProperties = pulsarContainerProperties;
		this.observationRegistry = observationRegistry;
	}

	public PulsarConsumerFactory<T> getPulsarConsumerFactory() {
		return this.pulsarConsumerFactory;
	}

	public PulsarContainerProperties getContainerProperties() {
		return this.pulsarContainerProperties;
	}

	public ObservationRegistry getObservationRegistry() {
		return this.observationRegistry;
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
	public void setupMessageListener(Object messageListener) {
		this.pulsarContainerProperties.setMessageListener(messageListener);
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

	@Override
	public void setAckTimeoutRedeliveryBackoff(RedeliveryBackoff redeliveryBackoff) {
		this.ackTimeoutRedeliveryBackoff = redeliveryBackoff;
	}

	public RedeliveryBackoff getNegativeAckRedeliveryBackoff() {
		return this.negativeAckRedeliveryBackoff;
	}

	public RedeliveryBackoff getAckTimeoutkRedeliveryBackoff() {
		return this.ackTimeoutRedeliveryBackoff;
	}

	@Override
	public void setDeadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
		this.deadLetterPolicy = deadLetterPolicy;
	}

	public DeadLetterPolicy getDeadLetterPolicy() {
		return this.deadLetterPolicy;
	}

	public PulsarConsumerErrorHandler<T> getPulsarConsumerErrorHandler() {
		return this.pulsarConsumerErrorHandler;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setPulsarConsumerErrorHandler(PulsarConsumerErrorHandler pulsarConsumerErrorHandler) {
		this.pulsarConsumerErrorHandler = pulsarConsumerErrorHandler;
	}

	@Override
	public void pause() {
		this.paused = true;
	}

	@Override
	public void resume() {
		this.paused = false;
	}

	protected boolean isPaused() {
		return this.paused;
	}

}

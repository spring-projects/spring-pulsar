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

import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.AbstractPulsarMessageContainer;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
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
public non-sealed abstract class AbstractPulsarMessageListenerContainer<T> extends AbstractPulsarMessageContainer
		implements PulsarMessageListenerContainer {

	private final PulsarConsumerFactory<T> pulsarConsumerFactory;

	private final PulsarContainerProperties pulsarContainerProperties;

	private final ObservationRegistry observationRegistry;

	protected final Object lifecycleMonitor = new Object();

	private volatile boolean paused;

	protected RedeliveryBackoff negativeAckRedeliveryBackoff;

	protected RedeliveryBackoff ackTimeoutRedeliveryBackoff;

	protected DeadLetterPolicy deadLetterPolicy;

	protected PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

	protected ConsumerBuilderCustomizer<T> consumerBuilderCustomizer;

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
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
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

	@SuppressWarnings("unchecked")
	@Override
	public void setConsumerCustomizer(ConsumerBuilderCustomizer<?> consumerBuilderCustomizer) {
		this.consumerBuilderCustomizer = (ConsumerBuilderCustomizer<T>) consumerBuilderCustomizer;
	}

	public ConsumerBuilderCustomizer<T> getConsumerBuilderCustomizer() {
		return this.consumerBuilderCustomizer;
	}

	@Override
	public void pause() {
		synchronized (this.lifecycleMonitor) {
			doPause();
		}
	}

	@Override
	public void resume() {
		synchronized (this.lifecycleMonitor) {
			doResume();
		}
	}

	protected boolean isPaused() {
		return this.paused;
	}

	protected void setPaused(boolean paused) {
		this.paused = paused;
	}

	protected abstract void doPause();

	protected abstract void doResume();

}

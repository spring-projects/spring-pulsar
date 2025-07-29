/*
 * Copyright 2022-present the original author or authors.
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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.core.AbstractPulsarMessageContainer;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.util.Assert;

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

	protected final ReentrantLock lifecycleLock = new ReentrantLock();

	private volatile boolean paused;

	protected @Nullable RedeliveryBackoff negativeAckRedeliveryBackoff;

	protected @Nullable RedeliveryBackoff ackTimeoutRedeliveryBackoff;

	protected @Nullable DeadLetterPolicy deadLetterPolicy;

	protected @Nullable PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

	protected @Nullable ConsumerBuilderCustomizer<T> consumerBuilderCustomizer;

	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		this.pulsarConsumerFactory = (PulsarConsumerFactory<T>) pulsarConsumerFactory;
		this.pulsarContainerProperties = pulsarContainerProperties;
	}

	public PulsarConsumerFactory<T> getPulsarConsumerFactory() {
		return this.pulsarConsumerFactory;
	}

	public PulsarContainerProperties getContainerProperties() {
		return this.pulsarContainerProperties;
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
		this.lifecycleLock.lock();
		try {
			if (!isRunning()) {
				Assert.state(this.pulsarContainerProperties.getMessageListener() instanceof PulsarRecordMessageListener,
						() -> "A " + PulsarRecordMessageListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void stop() {
		this.lifecycleLock.lock();
		try {
			if (isRunning()) {
				doStop();
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void setNegativeAckRedeliveryBackoff(@Nullable RedeliveryBackoff redeliveryBackoff) {
		this.negativeAckRedeliveryBackoff = redeliveryBackoff;
	}

	@Override
	public void setAckTimeoutRedeliveryBackoff(@Nullable RedeliveryBackoff redeliveryBackoff) {
		this.ackTimeoutRedeliveryBackoff = redeliveryBackoff;
	}

	public @Nullable RedeliveryBackoff getNegativeAckRedeliveryBackoff() {
		return this.negativeAckRedeliveryBackoff;
	}

	public @Nullable RedeliveryBackoff getAckTimeoutkRedeliveryBackoff() {
		return this.ackTimeoutRedeliveryBackoff;
	}

	@Override
	public void setDeadLetterPolicy(@Nullable DeadLetterPolicy deadLetterPolicy) {
		this.deadLetterPolicy = deadLetterPolicy;
	}

	public @Nullable DeadLetterPolicy getDeadLetterPolicy() {
		return this.deadLetterPolicy;
	}

	public @Nullable PulsarConsumerErrorHandler<T> getPulsarConsumerErrorHandler() {
		return this.pulsarConsumerErrorHandler;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setPulsarConsumerErrorHandler(@Nullable PulsarConsumerErrorHandler pulsarConsumerErrorHandler) {
		this.pulsarConsumerErrorHandler = pulsarConsumerErrorHandler;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setConsumerCustomizer(@Nullable ConsumerBuilderCustomizer<?> consumerBuilderCustomizer) {
		this.consumerBuilderCustomizer = (ConsumerBuilderCustomizer<T>) consumerBuilderCustomizer;
	}

	public @Nullable ConsumerBuilderCustomizer<T> getConsumerBuilderCustomizer() {
		return this.consumerBuilderCustomizer;
	}

	@Override
	public void pause() {
		this.lifecycleLock.lock();
		try {
			doPause();
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void resume() {
		this.lifecycleLock.lock();
		try {
			doResume();
		}
		finally {
			this.lifecycleLock.unlock();
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

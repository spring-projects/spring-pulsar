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

import java.util.ArrayList;
import java.util.List;

import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.observation.PulsarListenerObservationConvention;
import org.springframework.util.Assert;

import io.micrometer.observation.ObservationRegistry;

/**
 * Creates a concurrent execution context of {@link DefaultPulsarMessageListenerContainer}
 * instances based on the {@link #setConcurrency(int) concurrency}. Concurrency > 1 is not
 * allowed for exclusive subscriptions.
 *
 * @param <T> the payload type.
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 */
public class ConcurrentPulsarMessageListenerContainer<T> extends AbstractPulsarMessageListenerContainer<T> {

	private final List<DefaultPulsarMessageListenerContainer<T>> containers = new ArrayList<>();

	private int concurrency = 1;

	private final List<AsyncTaskExecutor> executors = new ArrayList<>();

	public ConcurrentPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		super(pulsarConsumerFactory, pulsarContainerProperties);
	}

	public int getConcurrency() {
		return this.concurrency;
	}

	/**
	 * The maximum number of concurrent {@link DefaultPulsarMessageListenerContainer}s
	 * running. Messages from within the same partition will be processed sequentially.
	 * Concurrency > 1 is not allowed for exclusive subscriptions.
	 * @param concurrency the concurrency.
	 */
	public void setConcurrency(int concurrency) {
		Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
		this.concurrency = concurrency;
	}

	@Override
	public void doStart() {
		if (!isRunning()) {

			PulsarContainerProperties containerProperties = getContainerProperties();
			if (containerProperties.getSubscriptionType() == SubscriptionType.Exclusive && this.concurrency > 1) {
				throw new IllegalStateException("concurrency > 1 is not allowed on Exclusive subscription type");
			}

			setRunning(true);

			// Set observation registry accordingly
			if (containerProperties.isObservationEnabled()) {
				ApplicationContext applicationContext = getApplicationContext();
				if (applicationContext == null) {
					this.logger.warn(() -> "Observations enabled but application context null - not recording");
				}
				else {
					applicationContext.getBeanProvider(ObservationRegistry.class)
						.ifUnique(containerProperties::setObservationRegistry);
					applicationContext.getBeanProvider(PulsarListenerObservationConvention.class)
						.ifUnique(containerProperties::setObservationConvention);
				}
			}

			for (int i = 0; i < this.concurrency; i++) {
				DefaultPulsarMessageListenerContainer<T> container = constructContainer(containerProperties);
				configureChildContainer(i, container);
				container.start();
				this.containers.add(container);
			}
		}
	}

	private DefaultPulsarMessageListenerContainer<T> constructContainer(PulsarContainerProperties containerProperties) {
		return new DefaultPulsarMessageListenerContainer<>(this.getPulsarConsumerFactory(), containerProperties);
	}

	private void configureChildContainer(int index, DefaultPulsarMessageListenerContainer<T> container) {
		String beanName = getBeanName();
		beanName = (beanName == null ? "consumer" : beanName) + "-" + index;
		container.setBeanName(beanName);
		ApplicationContext applicationContext = getApplicationContext();
		if (applicationContext != null) {
			container.setApplicationContext(applicationContext);
		}
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			container.setApplicationEventPublisher(publisher);
		}

		AsyncTaskExecutor exec = container.getContainerProperties().getConsumerTaskExecutor();
		if (exec == null) {
			exec = new SimpleAsyncTaskExecutor(beanName + "-C-");
			this.executors.add(exec);
			container.getContainerProperties().setConsumerTaskExecutor(exec);
		}
		container.setNegativeAckRedeliveryBackoff(this.negativeAckRedeliveryBackoff);
		container.setAckTimeoutRedeliveryBackoff(this.ackTimeoutRedeliveryBackoff);
		container.setDeadLetterPolicy(this.deadLetterPolicy);
		container.setPulsarConsumerErrorHandler(this.pulsarConsumerErrorHandler);
		container.setConsumerCustomizer(this.consumerBuilderCustomizer);
	}

	@Override
	public void doStop() {
		if (isRunning()) {
			setRunning(false);
			this.containers.forEach(DefaultPulsarMessageListenerContainer::stop);
			this.containers.clear();
		}
	}

	public List<DefaultPulsarMessageListenerContainer<T>> getContainers() {
		return this.containers;
	}

	@Override
	public void doPause() {
		if (!isPaused()) {
			setPaused(true);
			this.containers.forEach(AbstractPulsarMessageListenerContainer::pause);
		}
	}

	@Override
	public void doResume() {
		if (isPaused()) {
			setPaused(false);
			this.containers.forEach(AbstractPulsarMessageListenerContainer::resume);
		}
	}

}

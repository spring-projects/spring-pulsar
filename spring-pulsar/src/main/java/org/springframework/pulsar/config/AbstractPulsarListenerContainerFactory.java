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

package org.springframework.pulsar.config;

import org.apache.pulsar.client.api.Schema;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.AbstractPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.support.JavaUtils;
import org.springframework.pulsar.support.MessageConverter;

import io.micrometer.observation.ObservationRegistry;

/**
 * Base {@link PulsarListenerContainerFactory} implementation.
 *
 * @param <C> the {@link AbstractPulsarMessageListenerContainer} implementation type.
 * @param <T> Message payload type.
 * @author Soby Chacko
 * @author Chris Bono
 */
public abstract class AbstractPulsarListenerContainerFactory<C extends AbstractPulsarMessageListenerContainer<T>, T>
		implements PulsarListenerContainerFactory, ApplicationEventPublisherAware, ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarConsumerFactory<? super T> consumerFactory;

	private final PulsarContainerProperties containerProperties;

	private final ObservationRegistry observationRegistry;

	private Boolean autoStartup;

	private Integer phase;

	private MessageConverter messageConverter;

	private Boolean batchListener;

	private ApplicationEventPublisher applicationEventPublisher;

	private ApplicationContext applicationContext;

	protected AbstractPulsarListenerContainerFactory(PulsarConsumerFactory<? super T> consumerFactory,
			PulsarContainerProperties containerProperties, @Nullable ObservationRegistry observationRegistry) {
		this.consumerFactory = consumerFactory;
		this.containerProperties = containerProperties;
		this.observationRegistry = observationRegistry;
	}

	protected PulsarConsumerFactory<? super T> getConsumerFactory() {
		return this.consumerFactory;
	}

	protected ObservationRegistry getObservationRegistry() {
		return this.observationRegistry;
	}

	public PulsarContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set the message converter to use if dynamic argument type matching is needed.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	public void setBatchListener(Boolean batchListener) {
		this.batchListener = batchListener;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@SuppressWarnings("unchecked")
	@Override
	public C createListenerContainer(PulsarListenerEndpoint endpoint) {
		C instance = createContainerInstance(endpoint);
		JavaUtils.INSTANCE.acceptIfNotNull(endpoint.getId(), instance::setBeanName);
		if (endpoint instanceof AbstractPulsarListenerEndpoint) {
			configureEndpoint((AbstractPulsarListenerEndpoint<C>) endpoint);
		}

		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);
		// customizeContainer(instance);
		return instance;
	}

	protected abstract C createContainerInstance(PulsarListenerEndpoint endpoint);

	private void configureEndpoint(AbstractPulsarListenerEndpoint<C> aplEndpoint) {
		if (aplEndpoint.getBatchListener() == null) {
			JavaUtils.INSTANCE.acceptIfNotNull(this.batchListener, aplEndpoint::setBatchListener);
		}
	}

	protected void initializeContainer(C instance, PulsarListenerEndpoint endpoint) {
		PulsarContainerProperties instanceProperties = instance.getContainerProperties();

		if (instanceProperties.getSchemaType() == null) {
			JavaUtils.INSTANCE.acceptIfNotNull(this.containerProperties.getSchemaType(),
					instanceProperties::setSchemaType);
		}

		if (instanceProperties.getSchema() == null) {
			instanceProperties.setSchema(Schema.BYTES);
		}

		if (instanceProperties.getSubscriptionType() == null) {
			instanceProperties.setSubscriptionType(this.containerProperties.getSubscriptionType());
		}

		if (endpoint.getAckMode() != AckMode.BATCH) {
			instanceProperties.setAckMode(endpoint.getAckMode());
		}
		else if (this.containerProperties.getAckMode() != AckMode.BATCH) {
			instanceProperties.setAckMode(this.containerProperties.getAckMode());
		}

		Boolean autoStart = endpoint.getAutoStartup();
		if (autoStart != null) {
			instance.setAutoStartup(autoStart);
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}

		instanceProperties.setMaxNumMessages(this.containerProperties.getMaxNumMessages());
		instanceProperties.setMaxNumBytes(this.containerProperties.getMaxNumBytes());
		instanceProperties.setBatchTimeoutMillis(this.containerProperties.getBatchTimeoutMillis());
		instanceProperties.setObservationConvention(this.containerProperties.getObservationConvention());

		JavaUtils.INSTANCE.acceptIfNotNull(this.phase, instance::setPhase)
				.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
				.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher)
				.acceptIfNotNull(endpoint.getConsumerProperties(),
						instance.getContainerProperties()::setPulsarConsumerProperties);
	}

}

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

package org.springframework.pulsar.config;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.listener.AbstractPulsarMessageListenerContainer;
import org.springframework.pulsar.support.JavaUtils;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.PulsarContainerProperties;

/**
 * @author Soby Chacko
 */
public abstract class AbstractPulsarListenerContainerFactory<C extends AbstractPulsarMessageListenerContainer<T>, T>
		implements PulsarListenerContainerFactory<C>, ApplicationEventPublisherAware, InitializingBean,
		ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private final PulsarContainerProperties containerProperties = new PulsarContainerProperties();

	private PulsarConsumerFactory<? super T> consumerFactory;

	private Boolean autoStartup;

	private Integer phase;

	private MessageConverter messageConverter;

	private Boolean batchListener;

	private ApplicationEventPublisher applicationEventPublisher;

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}


	public void setPulsarConsumerFactory(PulsarConsumerFactory<? super T> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public PulsarConsumerFactory<? super T> getPulsarConsumerFactory() {
		return this.consumerFactory;
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


	public Boolean isBatchListener() {
		return this.batchListener;
	}


	public void setBatchListener(Boolean batchListener) {
		this.batchListener = batchListener;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}


	public PulsarContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void afterPropertiesSet() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public C createListenerContainer(PulsarListenerEndpoint endpoint) {
		C instance = createContainerInstance(endpoint);
		JavaUtils.INSTANCE
				.acceptIfNotNull(endpoint.getSubscriptionName(), instance::setBeanName);
		if (endpoint instanceof AbstractPulsarListenerEndpoint) {
			configureEndpoint((AbstractPulsarListenerEndpoint<C>) endpoint);
		}

		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);
		//customizeContainer(instance);
		return instance;
	}

	protected abstract C createContainerInstance(PulsarListenerEndpoint endpoint);

	private void configureEndpoint(AbstractPulsarListenerEndpoint<C> aplEndpoint) {

		if (aplEndpoint.getBatchListener() == null) {
			JavaUtils.INSTANCE
					.acceptIfNotNull(this.batchListener, aplEndpoint::setBatchListener);
		}
	}

	protected void initializeContainer(C instance, PulsarListenerEndpoint endpoint) {
		PulsarContainerProperties properties = instance.getPulsarContainerProperties();
		BeanUtils.copyProperties(this.containerProperties, properties, "topics", "messageListener", "batchReceive", "subscriptionName");

		Boolean autoStart = endpoint.getAutoStartup();
		if (autoStart != null) {
			instance.setAutoStartup(autoStart);
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}

		JavaUtils.INSTANCE
				.acceptIfNotNull(this.phase, instance::setPhase)
				.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
				.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher);
	}

}

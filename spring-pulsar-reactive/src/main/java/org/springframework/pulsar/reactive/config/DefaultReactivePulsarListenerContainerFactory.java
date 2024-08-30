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

package org.springframework.pulsar.reactive.config;

import java.util.Arrays;
import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.DefaultReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;
import org.springframework.pulsar.support.JavaUtils;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Concrete implementation for {@link ReactivePulsarListenerContainerFactory}.
 *
 * @param <T> Message payload type.
 * @author Christophe Bornet
 */
public class DefaultReactivePulsarListenerContainerFactory<T> implements ReactivePulsarListenerContainerFactory<T> {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarConsumerFactory<T> consumerFactory;

	private final ReactivePulsarContainerProperties<T> containerProperties;

	private Boolean autoStartup;

	private MessageConverter messageConverter;

	private Boolean fluxListener;

	public DefaultReactivePulsarListenerContainerFactory(ReactivePulsarConsumerFactory<T> consumerFactory,
			ReactivePulsarContainerProperties<T> containerProperties) {
		this.consumerFactory = consumerFactory;
		this.containerProperties = containerProperties;
	}

	protected ReactivePulsarConsumerFactory<T> getConsumerFactory() {
		return this.consumerFactory;
	}

	public ReactivePulsarContainerProperties<T> getContainerProperties() {
		return this.containerProperties;
	}

	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Set the message converter to use if dynamic argument type matching is needed.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	public void setFluxListener(Boolean fluxListener) {
		this.fluxListener = fluxListener;
	}

	@SuppressWarnings("unchecked")
	public DefaultReactivePulsarMessageListenerContainer<T> createContainerInstance(
			ReactivePulsarListenerEndpoint<T> endpoint) {

		ReactivePulsarContainerProperties<T> properties = new ReactivePulsarContainerProperties<>();
		properties.setSchemaResolver(this.getContainerProperties().getSchemaResolver());
		properties.setTopicResolver(this.getContainerProperties().getTopicResolver());
		properties.setSubscriptionType(this.getContainerProperties().getSubscriptionType());

		if (!CollectionUtils.isEmpty(endpoint.getTopics())) {
			properties.setTopics(endpoint.getTopics());
		}

		if (StringUtils.hasText(endpoint.getTopicPattern())) {
			properties.setTopicsPattern(endpoint.getTopicPattern());
		}

		if (StringUtils.hasText(endpoint.getSubscriptionName())) {
			properties.setSubscriptionName(endpoint.getSubscriptionName());
		}

		if (endpoint.getSubscriptionType() != null) {
			properties.setSubscriptionType(endpoint.getSubscriptionType());
		}
		// Default to Exclusive if not set on container props or endpoint
		if (properties.getSubscriptionType() == null) {
			properties.setSubscriptionType(SubscriptionType.Exclusive);
		}

		if (endpoint.getSchemaType() != null) {
			properties.setSchemaType(endpoint.getSchemaType());
		}
		else {
			properties.setSchemaType(this.containerProperties.getSchemaType());
		}

		if (properties.getSchema() == null) {
			properties.setSchema((Schema<T>) Schema.BYTES);
		}

		if (endpoint.getConcurrency() != null) {
			properties.setConcurrency(endpoint.getConcurrency());
		}
		else {
			properties.setConcurrency(this.containerProperties.getConcurrency());
		}

		if (endpoint.getUseKeyOrderedProcessing() != null) {
			properties.setUseKeyOrderedProcessing(endpoint.getUseKeyOrderedProcessing());
		}
		else {
			properties.setUseKeyOrderedProcessing(this.containerProperties.isUseKeyOrderedProcessing());
		}

		return new DefaultReactivePulsarMessageListenerContainer<>(this.getConsumerFactory(), properties);
	}

	@Override
	public DefaultReactivePulsarMessageListenerContainer<T> createListenerContainer(
			ReactivePulsarListenerEndpoint<T> endpoint) {
		DefaultReactivePulsarMessageListenerContainer<T> instance = createContainerInstance(endpoint);
		if (endpoint instanceof AbstractReactivePulsarListenerEndpoint) {
			configureEndpoint((AbstractReactivePulsarListenerEndpoint<T>) endpoint);
		}

		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);
		return instance;
	}

	private void configureEndpoint(AbstractReactivePulsarListenerEndpoint<T> aplEndpoint) {
		if (aplEndpoint.getFluxListener() == null) {
			JavaUtils.INSTANCE.acceptIfNotNull(this.fluxListener, aplEndpoint::setFluxListener);
		}
	}

	@Override
	public DefaultReactivePulsarMessageListenerContainer<T> createContainer(String... topics) {
		ReactivePulsarListenerEndpoint<T> endpoint = new ReactivePulsarListenerEndpoint<>() {

			@Override
			public List<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		DefaultReactivePulsarMessageListenerContainer<T> container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		return container;
	}

	@SuppressWarnings("unchecked")
	private void initializeContainer(DefaultReactivePulsarMessageListenerContainer<T> instance,
			ReactivePulsarListenerEndpoint<T> endpoint) {
		Boolean autoStart = endpoint.getAutoStartup();
		if (autoStart != null) {
			instance.setAutoStartup(autoStart);
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}

	}

}

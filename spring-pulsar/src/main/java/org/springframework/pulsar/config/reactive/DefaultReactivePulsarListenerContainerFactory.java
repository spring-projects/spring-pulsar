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

package org.springframework.pulsar.config.reactive;

import java.util.Arrays;
import java.util.List;

import org.apache.pulsar.client.api.Schema;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.reactive.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.listener.reactive.DefaultReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.listener.reactive.ReactivePulsarContainerProperties;
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

	private Integer concurrency;

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

	/**
	 * Specify the container concurrency.
	 * @param concurrency the number of handlers to create.
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	public DefaultReactivePulsarMessageListenerContainer<T> createContainerInstance(
			ReactivePulsarListenerEndpoint<T> endpoint) {

		ReactivePulsarContainerProperties<T> properties = new ReactivePulsarContainerProperties<>();

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

		properties.setSchemaType(endpoint.getSchemaType());

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
		ReactivePulsarListenerEndpoint<T> endpoint = new ReactivePulsarListenerEndpointAdapter<>() {

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
		ReactivePulsarContainerProperties<T> instanceProperties = instance.getContainerProperties();

		if (instanceProperties.getSchema() == null) {
			instanceProperties.setSchema((Schema<T>) Schema.BYTES);
		}

		if (instanceProperties.getSubscriptionType() == null) {
			instanceProperties.setSubscriptionType(this.containerProperties.getSubscriptionType());
		}

		Boolean autoStart = endpoint.getAutoStartup();
		if (autoStart != null) {
			instance.setAutoStartup(autoStart);
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}

		if (endpoint.getConcurrency() != null) {
			instanceProperties.setConcurrency(endpoint.getConcurrency());
		}
		else if (this.concurrency != null) {
			instanceProperties.setConcurrency(this.concurrency);
		}
	}

}

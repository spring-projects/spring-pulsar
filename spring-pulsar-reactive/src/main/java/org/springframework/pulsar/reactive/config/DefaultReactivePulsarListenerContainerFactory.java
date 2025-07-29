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

package org.springframework.pulsar.reactive.config;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

	private static final String SUBSCRIPTION_NAME_PREFIX = "org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#";

	private static final AtomicInteger COUNTER = new AtomicInteger();

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
		var containerProps = new ReactivePulsarContainerProperties<T>();
		var factoryProps = this.getContainerProperties();

		// Map factory props (defaults) to the container props
		containerProps.setSchemaResolver(factoryProps.getSchemaResolver());
		containerProps.setTopicResolver(factoryProps.getTopicResolver());
		containerProps.setSubscriptionType(factoryProps.getSubscriptionType());
		containerProps.setSubscriptionName(factoryProps.getSubscriptionName());
		containerProps.setSchemaType(factoryProps.getSchemaType());
		containerProps.setConcurrency(factoryProps.getConcurrency());
		containerProps.setUseKeyOrderedProcessing(factoryProps.isUseKeyOrderedProcessing());

		// Map relevant props from the endpoint to the container props
		if (!CollectionUtils.isEmpty(endpoint.getTopics())) {
			containerProps.setTopics(endpoint.getTopics());
		}
		if (StringUtils.hasText(endpoint.getTopicPattern())) {
			containerProps.setTopicsPattern(endpoint.getTopicPattern());
		}
		if (endpoint.getSubscriptionType() != null) {
			containerProps.setSubscriptionType(endpoint.getSubscriptionType());
		}
		// Default subscription type to Exclusive when not set elsewhere
		if (containerProps.getSubscriptionType() == null) {
			containerProps.setSubscriptionType(SubscriptionType.Exclusive);
		}
		if (StringUtils.hasText(endpoint.getSubscriptionName())) {
			containerProps.setSubscriptionName(endpoint.getSubscriptionName());
		}
		// Default subscription name to generated when not set elsewhere
		if (!StringUtils.hasText(containerProps.getSubscriptionName())) {
			var generatedName = SUBSCRIPTION_NAME_PREFIX + COUNTER.getAndIncrement();
			containerProps.setSubscriptionName(generatedName);
		}
		if (endpoint.getSchemaType() != null) {
			containerProps.setSchemaType(endpoint.getSchemaType());
		}
		// Default to BYTES if not set elsewhere
		if (containerProps.getSchema() == null) {
			containerProps.setSchema((Schema<T>) Schema.BYTES);
		}
		if (endpoint.getConcurrency() != null) {
			containerProps.setConcurrency(endpoint.getConcurrency());
		}
		if (endpoint.getUseKeyOrderedProcessing() != null) {
			containerProps.setUseKeyOrderedProcessing(endpoint.getUseKeyOrderedProcessing());
		}
		return new DefaultReactivePulsarMessageListenerContainer<>(this.getConsumerFactory(), containerProps);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public DefaultReactivePulsarMessageListenerContainer<T> createRegisteredContainer(
			ReactivePulsarListenerEndpoint<T> endpoint) {
		var instance = createContainerInstance(endpoint);
		if (endpoint instanceof AbstractReactivePulsarListenerEndpoint abstractReactiveEndpoint) {
			if (abstractReactiveEndpoint.getFluxListener() == null) {
				JavaUtils.INSTANCE.acceptIfNotNull(this.fluxListener, abstractReactiveEndpoint::setFluxListener);
			}
		}
		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);
		return instance;
	}

	@Override
	public DefaultReactivePulsarMessageListenerContainer<T> createContainer(String... topics) {
		ReactivePulsarListenerEndpoint<T> endpoint = new ReactivePulsarListenerEndpoint<>() {

			@Override
			public List<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		var container = createContainerInstance(endpoint);
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

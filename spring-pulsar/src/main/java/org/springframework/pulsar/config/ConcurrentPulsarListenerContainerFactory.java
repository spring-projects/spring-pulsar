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

package org.springframework.pulsar.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.ConcurrentPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Concrete implementation for {@link PulsarListenerContainerFactory}.
 *
 * @param <T> message type in the listener.
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Vedran Pavic
 * @author Daniel Szabo
 */
public class ConcurrentPulsarListenerContainerFactory<T>
		extends AbstractPulsarListenerContainerFactory<ConcurrentPulsarMessageListenerContainer<T>, T> {

	private static final String SUBSCRIPTION_NAME_PREFIX = "org.springframework.Pulsar.PulsarListenerEndpointContainer#";

	private static final AtomicInteger COUNTER = new AtomicInteger();

	public ConcurrentPulsarListenerContainerFactory(PulsarConsumerFactory<? super T> consumerFactory,
			PulsarContainerProperties containerProperties) {
		super(consumerFactory, containerProperties);
	}

	@Override
	public ConcurrentPulsarMessageListenerContainer<T> createContainer(String... topics) {
		PulsarListenerEndpoint endpoint = new PulsarListenerEndpoint() {

			@Override
			public Collection<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		ConcurrentPulsarMessageListenerContainer<T> container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		return container;
	}

	@Override
	protected ConcurrentPulsarMessageListenerContainer<T> createContainerInstance(PulsarListenerEndpoint endpoint) {
		var factoryProps = this.getContainerProperties();
		var containerProps = new PulsarContainerProperties();

		// Map factory props (defaults) to the container props
		containerProps.setConsumerTaskExecutor(factoryProps.getConsumerTaskExecutor());
		containerProps.setSchemaResolver(factoryProps.getSchemaResolver());
		containerProps.setTopicResolver(factoryProps.getTopicResolver());
		containerProps.setSubscriptionType(factoryProps.getSubscriptionType());
		containerProps.setSubscriptionName(factoryProps.getSubscriptionName());
		var factoryTxnProps = factoryProps.transactions();
		var containerTxnProps = containerProps.transactions();
		containerTxnProps.setEnabled(factoryTxnProps.isEnabled());
		containerTxnProps.setRequired(factoryTxnProps.isRequired());
		containerTxnProps.setTimeout(factoryTxnProps.getTimeout());
		containerTxnProps.setTransactionDefinition(factoryTxnProps.getTransactionDefinition());
		containerTxnProps.setTransactionManager(factoryTxnProps.getTransactionManager());

		// Map relevant props from the endpoint to the container props
		if (!CollectionUtils.isEmpty(endpoint.getTopics())) {
			containerProps.setTopics(new HashSet<>(endpoint.getTopics()));
		}
		if (StringUtils.hasText(endpoint.getTopicPattern())) {
			containerProps.setTopicsPattern(endpoint.getTopicPattern());
		}
		if (endpoint.isBatchListener()) {
			containerProps.setBatchListener(endpoint.isBatchListener());
		}
		if (StringUtils.hasText(endpoint.getSubscriptionName())) {
			containerProps.setSubscriptionName(endpoint.getSubscriptionName());
		}
		if (endpoint.getSubscriptionType() != null) {
			containerProps.setSubscriptionType(endpoint.getSubscriptionType());
		}
		// Default subscription name to generated when not set elsewhere
		if (!StringUtils.hasText(containerProps.getSubscriptionName())) {
			var generatedName = SUBSCRIPTION_NAME_PREFIX + COUNTER.getAndIncrement();
			containerProps.setSubscriptionName(generatedName);
		}
		// Default subscription type to Exclusive when not set elsewhere
		if (containerProps.getSubscriptionType() == null) {
			containerProps.setSubscriptionType(SubscriptionType.Exclusive);
		}
		containerProps.setSchemaType(endpoint.getSchemaType());

		return new ConcurrentPulsarMessageListenerContainer<>(this.getConsumerFactory(), containerProps);
	}

	@Override
	protected void initializeContainer(ConcurrentPulsarMessageListenerContainer<T> instance,
			PulsarListenerEndpoint endpoint) {
		super.initializeContainer(instance, endpoint);
		if (endpoint.getConcurrency() != null) {
			instance.setConcurrency(endpoint.getConcurrency());
		}
		else if (getContainerProperties().getConcurrency() > 0) {
			instance.setConcurrency(getContainerProperties().getConcurrency());
		}
	}

}

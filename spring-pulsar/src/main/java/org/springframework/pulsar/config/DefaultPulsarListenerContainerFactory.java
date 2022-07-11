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

import java.util.Arrays;
import java.util.Collection;

import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.util.StringUtils;

/**
 * Concrete implementation for {@link PulsarListenerContainerFactory}.
 *
 * @param <C> container implementation type.
 * @param <T> message type in the listener.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarListenerContainerFactory<C, T> extends AbstractPulsarListenerContainerFactory<DefaultPulsarMessageListenerContainer<T>, T> {

	@Override
	protected DefaultPulsarMessageListenerContainer<T> createContainerInstance(PulsarListenerEndpoint endpoint) {

		PulsarContainerProperties properties = new PulsarContainerProperties();
		Collection<String> topics = endpoint.getTopics();

		if (!topics.isEmpty()) {
			final String[] topics1 = topics.toArray(new String[0]);
			properties.setTopics(topics1);
		}

		final String subscriptionName = endpoint.getSubscriptionName();

		if (StringUtils.hasText(subscriptionName)) {
			properties.setSubscriptionName(endpoint.getSubscriptionName());
		}
		if (endpoint.isBatchListener()) {
			properties.setBatchListener(endpoint.isBatchListener());
		}
		final SubscriptionType subscriptionType = endpoint.getSubscriptionType();
		if (subscriptionType != null) {
			properties.setSubscriptionType(subscriptionType);
		}

		properties.setSchemaType(endpoint.getSchemaType());

		return new DefaultPulsarMessageListenerContainer<T>(getPulsarConsumerFactory(), properties);
	}

	@Override
	protected void initializeContainer(DefaultPulsarMessageListenerContainer<T> instance,
									PulsarListenerEndpoint endpoint) {

		super.initializeContainer(instance, endpoint);
	}

	@Override
	public DefaultPulsarMessageListenerContainer<T> createContainer(String... topics) {
		PulsarListenerEndpoint endpoint = new PulsarListenerEndpointAdapter() {

			@Override
			public Collection<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		DefaultPulsarMessageListenerContainer<T> container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		//customizeContainer(container);
		return container;
	}
}

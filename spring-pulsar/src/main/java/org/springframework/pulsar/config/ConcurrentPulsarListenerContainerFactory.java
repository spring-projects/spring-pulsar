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

import org.springframework.pulsar.listener.ConcurrentPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.util.StringUtils;

/**
 * Concrete implementation for {@link PulsarListenerContainerFactory}.
 *
 * @param <T> message type in the listener.
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
public class ConcurrentPulsarListenerContainerFactory<T>
		extends AbstractPulsarListenerContainerFactory<ConcurrentPulsarMessageListenerContainer<T>, T> {

	private Integer concurrency;

	/**
	 * Specify the container concurrency.
	 * @param concurrency the number of consumers to create.
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	@Override
	protected ConcurrentPulsarMessageListenerContainer<T> createContainerInstance(PulsarListenerEndpoint endpoint) {
		PulsarContainerProperties properties = new PulsarContainerProperties();
		Collection<String> topics = endpoint.getTopics();
		String topicPattern = endpoint.getTopicPattern();

		if (!topics.isEmpty()) {
			final String[] topics1 = topics.toArray(new String[0]);
			properties.setTopics(topics1);
		}
		if (StringUtils.hasText(topicPattern)) {
			properties.setTopicsPattern(topicPattern);
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
		return new ConcurrentPulsarMessageListenerContainer<T>(getPulsarConsumerFactory(), properties);
	}

	@Override
	protected void initializeContainer(ConcurrentPulsarMessageListenerContainer<T> instance,
			PulsarListenerEndpoint endpoint) {

		super.initializeContainer(instance, endpoint);
		Integer conc = endpoint.getConcurrency();
		if (conc != null) {
			instance.setConcurrency(conc);
		}
		else if (this.concurrency != null) {
			instance.setConcurrency(this.concurrency);
		}
	}

	@Override
	public ConcurrentPulsarMessageListenerContainer<T> createContainer(String... topics) {
		PulsarListenerEndpoint endpoint = new PulsarListenerEndpointAdapter() {

			@Override
			public Collection<String> getTopics() {
				return Arrays.asList(topics);
			}

		};
		ConcurrentPulsarMessageListenerContainer<T> container = createContainerInstance(endpoint);
		initializeContainer(container, endpoint);
		// customizeContainer(container);
		return container;
	}

}

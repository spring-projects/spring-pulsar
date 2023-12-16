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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

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
 */
public class ConcurrentPulsarListenerContainerFactory<T>
		extends AbstractPulsarListenerContainerFactory<ConcurrentPulsarMessageListenerContainer<T>, T> {

	private Integer concurrency;

	public ConcurrentPulsarListenerContainerFactory(PulsarConsumerFactory<? super T> consumerFactory,
			PulsarContainerProperties containerProperties) {
		super(consumerFactory, containerProperties);
	}

	/**
	 * Specify the container concurrency.
	 * @param concurrency the number of consumers to create.
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
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
		// customizeContainer(container);
		return container;
	}

	@Override
	protected ConcurrentPulsarMessageListenerContainer<T> createContainerInstance(PulsarListenerEndpoint endpoint) {

		PulsarContainerProperties properties = new PulsarContainerProperties();
		properties.setSchemaResolver(this.getContainerProperties().getSchemaResolver());
		properties.setTopicResolver(this.getContainerProperties().getTopicResolver());

		if (!CollectionUtils.isEmpty(endpoint.getTopics())) {
			properties.setTopics(new HashSet<>(endpoint.getTopics()));
		}

		if (StringUtils.hasText(endpoint.getTopicPattern())) {
			properties.setTopicsPattern(endpoint.getTopicPattern());
		}

		if (StringUtils.hasText(endpoint.getSubscriptionName())) {
			properties.setSubscriptionName(endpoint.getSubscriptionName());
		}

		if (endpoint.isBatchListener()) {
			properties.setBatchListener(endpoint.isBatchListener());
		}

		if (endpoint.getSubscriptionType() != null) {
			properties.setSubscriptionType(endpoint.getSubscriptionType());
		}

		properties.setSchemaType(endpoint.getSchemaType());

		return new ConcurrentPulsarMessageListenerContainer<>(this.getConsumerFactory(), properties);
	}

	@Override
	protected void initializeContainer(ConcurrentPulsarMessageListenerContainer<T> instance,
			PulsarListenerEndpoint endpoint) {
		super.initializeContainer(instance, endpoint);
		if (endpoint.getConcurrency() != null) {
			instance.setConcurrency(endpoint.getConcurrency());
		}
		else if (this.concurrency != null) {
			instance.setConcurrency(this.concurrency);
		}
	}

}

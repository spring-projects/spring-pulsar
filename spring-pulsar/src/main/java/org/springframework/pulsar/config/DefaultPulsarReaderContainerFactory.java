/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.reader.DefaultPulsarReaderListenerContainer;
import org.springframework.pulsar.reader.PulsarReaderContainerProperties;
import org.springframework.pulsar.reader.PulsarReaderListenerContainer;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Concrete implementation for {@link PulsarReaderContainerFactory}.
 *
 * @param <T> message type in the listener.
 * @author Soby Chacko
 */
public class DefaultPulsarReaderContainerFactory<T>
		extends AbstractPulsarReaderContainerFactory<DefaultPulsarReaderListenerContainer<T>, T> {

	public DefaultPulsarReaderContainerFactory(PulsarReaderFactory<? super T> readerFactory,
			PulsarReaderContainerProperties containerProperties) {
		super(readerFactory, containerProperties);
	}

	@Override
	protected DefaultPulsarReaderListenerContainer<T> createContainerInstance(
			PulsarReaderEndpoint<PulsarReaderListenerContainer> endpoint) {

		PulsarReaderContainerProperties properties = new PulsarReaderContainerProperties();
		properties.setSchemaResolver(this.getContainerProperties().getSchemaResolver());

		if (!CollectionUtils.isEmpty(endpoint.getTopics())) {
			properties.setTopics(endpoint.getTopics());
		}

		if (StringUtils.hasText(endpoint.getSubscriptionName())) {
			properties.setSubscriptionName(endpoint.getSubscriptionName());
		}

		properties.setSchemaType(endpoint.getSchemaType());
		properties.setStartMessageId(endpoint.getStartMessageId());

		return new DefaultPulsarReaderListenerContainer<>(this.getReaderFactory(), properties);
	}

	@Override
	protected void initializeContainer(DefaultPulsarReaderListenerContainer<T> instance,
			PulsarReaderEndpoint<PulsarReaderListenerContainer> endpoint) {
		super.initializeContainer(instance, endpoint);
	}

	@Override
	public DefaultPulsarReaderListenerContainer<T> createReaderContainer(String... topics) {
		// TODO
		return null;
	}

}

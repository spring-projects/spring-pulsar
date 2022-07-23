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

package org.springframework.pulsar.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarProducerFactory}.
 *
 * @param <T> producer type.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarProducerFactory<T> implements PulsarProducerFactory<T> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final Map<String, Object> producerConfig = new HashMap<>();

	private final PulsarClient pulsarClient;

	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> config) {
		this.pulsarClient = pulsarClient;
		if (!CollectionUtils.isEmpty(config)) {
			this.producerConfig.putAll(config);
		}
	}

	@Override
	public Producer<T> createProducer(String topic, Schema<T> schema) throws PulsarClientException {
		return createProducer(topic, schema, null);
	}

	@Override
	public Producer<T> createProducer(String topic, Schema<T> schema, MessageRouter messageRouter) throws PulsarClientException {
		return doCreateProducer(topic, schema, messageRouter);
	}

	protected Producer<T> doCreateProducer(String topic, Schema<T> schema, MessageRouter messageRouter) throws PulsarClientException {
		final String resolvedTopic = ProducerUtils.resolveTopicName(topic, this);
		this.logger.trace(() -> String.format("Creating producer for '%s' topic", resolvedTopic));
		final ProducerBuilder<T> producerBuilder = this.pulsarClient.newProducer(schema);
		if (!CollectionUtils.isEmpty(this.producerConfig)) {
			producerBuilder.loadConf(this.producerConfig);
		}
		producerBuilder.topic(resolvedTopic);
		if (messageRouter != null) {
			producerBuilder.messageRouter(messageRouter);
		}
		return producerBuilder.create();
	}

	@Override
	public Map<String, Object> getProducerConfig() {
		return this.producerConfig;
	}
}

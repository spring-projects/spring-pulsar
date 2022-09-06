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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;

import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link PulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the consumer.
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public class DefaultPulsarConsumerFactory<T> implements PulsarConsumerFactory<T> {

	private final Map<String, Object> consumerConfig = new HashMap<>();

	private final List<Consumer<T>> consumers = new ArrayList<>();

	private PulsarClient pulsarClient;

	public DefaultPulsarConsumerFactory(PulsarClient pulsarClient, Map<String, Object> consumerConfig) {
		this.pulsarClient = pulsarClient;
		if (!CollectionUtils.isEmpty(consumerConfig)) {
			this.consumerConfig.putAll(consumerConfig);
		}
	}

	@Override
	public Consumer<T> createConsumer(Schema<T> schema, Map<String, Object> propertiesToOverride)
			throws PulsarClientException {

		final ConsumerBuilder<T> consumerBuilder = this.pulsarClient.newConsumer(schema);

		final Map<String, Object> properties = new HashMap<>(this.consumerConfig);
		properties.putAll(propertiesToOverride);

		if (!CollectionUtils.isEmpty(properties)) {
			consumerBuilder.loadConf(properties);
		}
		Consumer<T> consumer = consumerBuilder.subscribe();
		this.consumers.add(consumer);
		return consumer;
	}

	@Override
	public Consumer<T> createConsumer(Schema<T> schema, BatchReceivePolicy batchReceivePolicy,
			Map<String, Object> propertiesToOverride) throws PulsarClientException {

		final ConsumerBuilder<T> consumerBuilder = this.pulsarClient.newConsumer(schema);
		final Map<String, Object> properties = new HashMap<>(this.consumerConfig);
		properties.putAll(propertiesToOverride);

		// Remove deadLetterPolicy from the properties here and save it to re-apply after
		// calling `loadConf` (https://github.com/apache/pulsar/issues/11646)
		DeadLetterPolicy deadLetterPolicy = null;
		if (properties.containsKey("deadLetterPolicy")) {
			deadLetterPolicy = (DeadLetterPolicy) properties.remove("deadLetterPolicy");
		}

		if (!CollectionUtils.isEmpty(properties)) {
			consumerBuilder.loadConf(properties);
		}

		if (deadLetterPolicy != null) {
			consumerBuilder.deadLetterPolicy(deadLetterPolicy);
		}

		if (properties.containsKey("negativeAckRedeliveryBackoff")) {
			final RedeliveryBackoff negativeAckRedeliveryBackoff = (RedeliveryBackoff) properties
					.get("negativeAckRedeliveryBackoff");
			consumerBuilder.negativeAckRedeliveryBackoff(negativeAckRedeliveryBackoff);
		}

		consumerBuilder.batchReceivePolicy(batchReceivePolicy);
		Consumer<T> consumer = consumerBuilder.subscribe();
		this.consumers.add(consumer);
		return consumer;
	}

	public Map<String, Object> getConsumerConfig() {
		return this.consumerConfig;
	}

}

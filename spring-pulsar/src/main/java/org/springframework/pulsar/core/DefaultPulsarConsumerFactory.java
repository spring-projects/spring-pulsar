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

package org.springframework.pulsar.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link PulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the consumer.
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Christophe Bornet
 */
public class DefaultPulsarConsumerFactory<T> implements PulsarConsumerFactory<T> {

	private final Map<String, Object> consumerConfig;

	private final PulsarClient pulsarClient;

	/**
	 * Construct a consumer factory instance.
	 * @param pulsarClient the client used to consume
	 * @param consumerConfig default configuration to apply to the created consumer or
	 * empty map to use no default configuration
	 */
	public DefaultPulsarConsumerFactory(PulsarClient pulsarClient, Map<String, Object> consumerConfig) {
		this.pulsarClient = pulsarClient;
		this.consumerConfig = Collections.unmodifiableMap(consumerConfig);
	}

	@Override
	public Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics,
			@Nullable String subscriptionName, ConsumerBuilderCustomizer<T> customizer) throws PulsarClientException {
		return createConsumer(schema, topics, subscriptionName, null,
				customizer != null ? Collections.singletonList(customizer) : null);
	}

	@Override
	public Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics,
			@Nullable String subscriptionName, @Nullable Map<String, String> metadataProperties,
			@Nullable List<ConsumerBuilderCustomizer<T>> customizers) throws PulsarClientException {
		Objects.requireNonNull(schema, "Schema must be specified");
		ConsumerBuilder<T> consumerBuilder = this.pulsarClient.newConsumer(schema);
		Map<String, Object> config = new HashMap<>(this.consumerConfig);
		if (topics != null) {
			config.put("topicNames", new HashSet<>(topics));
		}
		if (metadataProperties != null) {
			config.put("properties", new TreeMap<>(metadataProperties));
		}
		if (subscriptionName != null) {
			config.put("subscriptionName", subscriptionName);
		}
		ConsumerBuilderConfigurationUtil.loadConf(consumerBuilder, config);
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach(customizer -> customizer.customize(consumerBuilder));
		}
		return consumerBuilder.subscribe();
	}

	public Map<String, Object> getConsumerConfig() {
		return this.consumerConfig;
	}

}

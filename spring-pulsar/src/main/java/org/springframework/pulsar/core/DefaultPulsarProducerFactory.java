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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarProducerFactory}.
 *
 * @param <T> producer type.
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 */
public class DefaultPulsarProducerFactory<T> implements PulsarProducerFactory<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final Map<String, Object> producerConfig;

	private final PulsarClient pulsarClient;

	private final TopicResolver topicResolver;

	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> config) {
		this(pulsarClient, config, new DefaultTopicResolver());
	}

	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> config,
			TopicResolver topicResolver) {
		this.pulsarClient = pulsarClient;
		this.producerConfig = Collections.unmodifiableMap(config);
		this.topicResolver = topicResolver;
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic) throws PulsarClientException {
		return doCreateProducer(schema, topic, null, null);
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic,
			@Nullable ProducerBuilderCustomizer<T> customizer) throws PulsarClientException {
		return doCreateProducer(schema, topic, null, customizer != null ? Collections.singletonList(customizer) : null);
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers)
			throws PulsarClientException {
		return doCreateProducer(schema, topic, encryptionKeys, customizers);
	}

	/**
	 * Create the actual producer.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic the producer will send messages to or {@code null} to use
	 * the default topic
	 * @param encryptionKeys the encryption keys used by the producer, replacing the
	 * default encryption keys or {@code null} to use the default encryption keys. Beware
	 * that {@link ProducerBuilder} only has {@link ProducerBuilder#addEncryptionKey} and
	 * doesn't have methods to replace the encryption keys.
	 * @param customizers the optional list of customizers to apply to the producer
	 * builder
	 * @return the created producer
	 * @throws PulsarClientException if any error occurs
	 */
	protected Producer<T> doCreateProducer(Schema<T> schema, @Nullable String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers)
			throws PulsarClientException {
		Objects.requireNonNull(schema, "Schema must be specified");
		String resolvedTopic = resolveTopicName(topic);
		this.logger.trace(() -> "Creating producer for '%s' topic".formatted(resolvedTopic));
		ProducerBuilder<T> producerBuilder = this.pulsarClient.newProducer(schema);

		Map<String, Object> config = new HashMap<>(this.producerConfig);

		// Replace default keys - workaround as they can't be replaced through the builder
		if (encryptionKeys != null) {
			config.put("encryptionKeys", encryptionKeys);
		}
		ProducerBuilderConfigurationUtil.loadConf(producerBuilder, config);
		producerBuilder.topic(resolvedTopic);

		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(producerBuilder));
		}
		// make sure the customizer do not override the topic
		producerBuilder.topic(resolvedTopic);

		return producerBuilder.create();
	}

	protected String resolveTopicName(String userSpecifiedTopic) {
		String defaultTopic = Objects.toString(getProducerConfig().get("topicName"), null);
		return this.topicResolver.resolveTopic(userSpecifiedTopic, () -> defaultTopic).orElseThrow();
	}

	@Override
	public Map<String, Object> getProducerConfig() {
		return this.producerConfig;
	}

}

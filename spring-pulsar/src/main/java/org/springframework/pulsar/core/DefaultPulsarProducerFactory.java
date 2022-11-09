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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageRouter;
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

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final Map<String, Object> producerConfig;

	private final PulsarClient pulsarClient;

	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> config) {
		this.pulsarClient = pulsarClient;
		this.producerConfig = Collections.unmodifiableMap(config);
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema) throws PulsarClientException {
		return doCreateProducer(schema, null, null, null);
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic) throws PulsarClientException {
		return doCreateProducer(schema, topic, null, null);
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
		String resolvedTopic = ProducerUtils.resolveTopicName(topic, this);
		this.logger.trace(() -> String.format("Creating producer for '%s' topic", resolvedTopic));
		ProducerBuilder<T> producerBuilder = this.pulsarClient.newProducer(schema);

		Map<String, Object> config = new HashMap<>(this.producerConfig);

		if (encryptionKeys != null) {
			config.put("encryptionKeys", encryptionKeys);
		}
		loadConf(producerBuilder, config);
		producerBuilder.topic(resolvedTopic);

		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(producerBuilder));
		}
		return producerBuilder.create();
	}

	@Override
	public Map<String, Object> getProducerConfig() {
		return this.producerConfig;
	}

	private static <T> void loadConf(ProducerBuilder<T> producerBuilder, Map<String, Object> config) {
		producerBuilder.loadConf(config);

		// Workaround because encryptionKeys are not loaded by loadConf and can't be
		// replaced through the builder
		if (config.containsKey("encryptionKeys")) {
			@SuppressWarnings("unchecked")
			Collection<String> keys = (Collection<String>) config.get("encryptionKeys");
			keys.forEach(producerBuilder::addEncryptionKey);
		}

		// Set non-serializable fields that not loaded by loadConf
		if (config.containsKey("customMessageRouter")) {
			producerBuilder.messageRouter((MessageRouter) config.get("customMessageRouter"));
		}
		if (config.containsKey("batcherBuilder")) {
			producerBuilder.batcherBuilder((BatcherBuilder) config.get("batcherBuilder"));
		}
		if (config.containsKey("cryptoKeyReader")) {
			producerBuilder.cryptoKeyReader((CryptoKeyReader) config.get("cryptoKeyReader"));
		}
	}

}

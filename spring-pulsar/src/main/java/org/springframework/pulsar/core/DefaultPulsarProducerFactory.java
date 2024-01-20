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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;
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

	private final PulsarClient pulsarClient;

	@Nullable
	private final String defaultTopic;

	@Nullable
	private final List<ProducerBuilderCustomizer<T>> defaultConfigCustomizers;

	private final TopicResolver topicResolver;

	/**
	 * Construct a producer factory that uses a default topic resolver.
	 * @param pulsarClient the client used to create the producers
	 */
	public DefaultPulsarProducerFactory(PulsarClient pulsarClient) {
		this(pulsarClient, null, null, new DefaultTopicResolver());
	}

	/**
	 * Construct a producer factory that uses a default topic resolver.
	 * @param pulsarClient the client used to create the producers
	 * @param defaultTopic the default topic to use for the producers
	 */
	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, @Nullable String defaultTopic) {
		this(pulsarClient, defaultTopic, null, new DefaultTopicResolver());
	}

	/**
	 * Construct a producer factory that uses a default topic resolver.
	 * @param pulsarClient the client used to create the producers
	 * @param defaultTopic the default topic to use for the producers
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * created producers
	 */
	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, @Nullable String defaultTopic,
			@Nullable List<ProducerBuilderCustomizer<T>> defaultConfigCustomizers) {
		this(pulsarClient, defaultTopic, defaultConfigCustomizers, new DefaultTopicResolver());
	}

	/**
	 * Construct a producer factory that uses the specified parameters.
	 * @param pulsarClient the client used to create the producers
	 * @param defaultTopic the default topic to use for the producers
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * created producers
	 * @param topicResolver the topic resolver to use
	 */
	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, @Nullable String defaultTopic,
			@Nullable List<ProducerBuilderCustomizer<T>> defaultConfigCustomizers, TopicResolver topicResolver) {
		this.pulsarClient = pulsarClient;
		this.defaultTopic = defaultTopic;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
		this.topicResolver = topicResolver;
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic) {
		return doCreateProducer(schema, topic, null, null);
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic,
			@Nullable ProducerBuilderCustomizer<T> customizer) {
		try {
			return doCreateProducer(schema, topic, null,
					customizer != null ? Collections.singletonList(customizer) : null);
		}
		catch (PulsarException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new PulsarException(PulsarClientException.unwrap(ex));
		}
	}

	@Override
	public Producer<T> createProducer(Schema<T> schema, @Nullable String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		try {
			return doCreateProducer(schema, topic, encryptionKeys, customizers);
		}
		catch (PulsarException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new PulsarException(PulsarClientException.unwrap(ex));
		}
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
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		var resolvedTopic = resolveTopicName(topic);
		this.logger.trace(() -> "Creating producer for '%s' topic".formatted(resolvedTopic));
		var producerBuilder = this.pulsarClient.newProducer(schema);

		// Apply the default config customizer (preserve the topic)
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer) -> customizer.customize(producerBuilder));
		}
		producerBuilder.topic(resolvedTopic);

		// Replace default keys - workaround as they can't be replaced through the builder
		maybeSetEncryptionKeys(producerBuilder, encryptionKeys);

		// Apply any user-specified customizers (preserve the topic)
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(producerBuilder));
		}
		producerBuilder.topic(resolvedTopic);

		try {
			return producerBuilder.create();
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
	}

	protected String resolveTopicName(String userSpecifiedTopic) {
		return this.topicResolver.resolveTopic(userSpecifiedTopic, this::getDefaultTopic).orElseThrow();
	}

	@Override
	public String getDefaultTopic() {
		return this.defaultTopic;
	}

	private void maybeSetEncryptionKeys(ProducerBuilder<T> builder, @Nullable Collection<String> encryptionKeys) {
		if (encryptionKeys != null) {
			var builderImpl = (ProducerBuilderImpl<T>) builder;
			builderImpl.getConf().setEncryptionKeys(new HashSet<>(encryptionKeys));
		}
	}

}

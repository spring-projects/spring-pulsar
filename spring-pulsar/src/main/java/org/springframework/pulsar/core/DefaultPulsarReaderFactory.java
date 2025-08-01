/*
 * Copyright 2023-present the original author or authors.
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ReaderBuilderImpl;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarReaderFactory}.
 *
 * @param <T> message type
 * @author Soby Chacko
 */
public class DefaultPulsarReaderFactory<T> implements PulsarReaderFactory<T> {

	private final PulsarClient pulsarClient;

	private @Nullable final List<ReaderBuilderCustomizer<T>> defaultConfigCustomizers;

	private @Nullable PulsarTopicBuilder topicBuilder;

	/**
	 * Construct a reader factory instance with no default configuration.
	 * @param pulsarClient the client used to consume
	 */
	public DefaultPulsarReaderFactory(PulsarClient pulsarClient) {
		this(pulsarClient, null);
	}

	/**
	 * Construct a reader factory instance.
	 * @param pulsarClient the client used to consume
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * readers or null to use no default configuration
	 */
	public DefaultPulsarReaderFactory(PulsarClient pulsarClient,
			@Nullable List<ReaderBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.pulsarClient = pulsarClient;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	/**
	 * Non-fully-qualified topic names specified on the created readers will be
	 * automatically fully-qualified with a default prefix
	 * ({@code domain://tenant/namespace}) according to the specified topic builder.
	 * @param topicBuilder the topic builder used to fully qualify topic names or null to
	 * not fully qualify topic names
	 * @since 1.2.0
	 */
	public void setTopicBuilder(@Nullable PulsarTopicBuilder topicBuilder) {
		this.topicBuilder = topicBuilder;
	}

	@Override
	public Reader<T> createReader(@Nullable List<String> topics, @Nullable MessageId messageId, Schema<T> schema,
			@Nullable List<ReaderBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		ReaderBuilder<T> readerBuilder = this.pulsarClient.newReader(schema);

		// Apply the default config customizer (preserve the topics)
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(readerBuilder)));
		}

		if (!CollectionUtils.isEmpty(topics)) {
			replaceTopicsOnBuilder(readerBuilder, topics);
		}

		if (messageId != null) {
			readerBuilder.startMessageId(messageId);
		}

		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach(customizer -> customizer.customize(readerBuilder));
		}

		try {
			return readerBuilder.create();
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
	}

	private void replaceTopicsOnBuilder(ReaderBuilder<T> builder, Collection<String> topics) {
		if (this.topicBuilder != null) {
			topics = topics.stream().map(this.topicBuilder::getFullyQualifiedNameForTopic).toList();
		}
		var builderImpl = (ReaderBuilderImpl<T>) builder;
		builderImpl.getConf().setTopicNames(new HashSet<>(topics));
	}

}

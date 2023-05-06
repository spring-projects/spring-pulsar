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

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarReaderFactory}.
 *
 * @param <T> message type
 * @author Soby Chacko
 */
public class DefaultPulsarReaderFactory<T> implements PulsarReaderFactory<T> {

	private final PulsarClient pulsarClient;

	@Nullable
	private final ReaderBuilderCustomizer<T> defaultConfigCustomizer;

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
	 * @param defaultConfigCustomizer the default configuration to apply to the readers or
	 * null to use no default configuration
	 */
	public DefaultPulsarReaderFactory(PulsarClient pulsarClient,
			@Nullable ReaderBuilderCustomizer<T> defaultConfigCustomizer) {
		this.pulsarClient = pulsarClient;
		this.defaultConfigCustomizer = defaultConfigCustomizer;
	}

	@Override
	public Reader<T> createReader(@Nullable List<String> topics, @Nullable MessageId messageId, Schema<T> schema,
			@Nullable List<ReaderBuilderCustomizer<T>> customizers) throws PulsarClientException {
		Objects.requireNonNull(schema, "Schema must be specified");
		ReaderBuilder<T> readerBuilder = this.pulsarClient.newReader(schema);

		// Apply the default config customizer (preserve the topics)
		if (this.defaultConfigCustomizer != null) {
			this.defaultConfigCustomizer.customize(readerBuilder);
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

		return readerBuilder.create();
	}

	private void replaceTopicsOnBuilder(ReaderBuilder<T> builder, Collection<String> topics) {
		var builderImpl = (ReaderBuilderImpl<T>) builder;
		builderImpl.getConf().setTopicNames(new HashSet<>(topics));
	}

}

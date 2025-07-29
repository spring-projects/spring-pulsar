/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.reactive.core;

import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderBuilder;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.PulsarTopicBuilder;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarReaderFactory}.
 *
 * @param <T> underlying payload type for the reactive reader.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class DefaultReactivePulsarReaderFactory<T> implements ReactivePulsarReaderFactory<T> {

	private final ReactivePulsarClient reactivePulsarClient;

	@Nullable
	private final List<ReactiveMessageReaderBuilderCustomizer<T>> defaultConfigCustomizers;

	@Nullable
	private PulsarTopicBuilder topicBuilder;

	/**
	 * Construct an instance.
	 * @param reactivePulsarClient the reactive client
	 * @param defaultConfigCustomizers the optional list of customizers that defines the
	 * default configuration for each created reader.
	 */
	public DefaultReactivePulsarReaderFactory(ReactivePulsarClient reactivePulsarClient,
			List<ReactiveMessageReaderBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.reactivePulsarClient = reactivePulsarClient;
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
	public ReactiveMessageReader<T> createReader(Schema<T> schema) {
		return createReader(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageReader<T> createReader(Schema<T> schema,
			List<ReactiveMessageReaderBuilderCustomizer<T>> customizers) {
		ReactiveMessageReaderBuilder<T> readerBuilder = this.reactivePulsarClient.messageReader(schema);
		// Apply the default customizers
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(readerBuilder)));
		}
		// Apply the user specified customizers
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(readerBuilder));
		}
		if (this.topicBuilder != null) {
			this.ensureTopicNamesFullyQualified(readerBuilder);
		}
		return readerBuilder.build();
	}

	protected void ensureTopicNamesFullyQualified(ReactiveMessageReaderBuilder<T> readerBuilder) {
		var mutableSpec = readerBuilder.getMutableSpec();
		var topics = mutableSpec.getTopicNames();
		if (!CollectionUtils.isEmpty(topics)) {
			var fullyQualifiedTopics = topics.stream().map(this.topicBuilder::getFullyQualifiedNameForTopic).toList();
			mutableSpec.setTopicNames(fullyQualifiedTopics);
		}
	}

}

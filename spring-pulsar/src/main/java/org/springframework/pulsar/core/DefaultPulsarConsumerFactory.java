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

package org.springframework.pulsar.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link PulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the consumer.
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Chris Bono
 * @author Jonas Geiregat
 */
public class DefaultPulsarConsumerFactory<T> implements PulsarConsumerFactory<T> {

	private final PulsarClient pulsarClient;

	private @Nullable final List<ConsumerBuilderCustomizer<T>> defaultConfigCustomizers;

	private @Nullable PulsarTopicBuilder topicBuilder;

	/**
	 * Construct a consumer factory instance.
	 * @param pulsarClient the client used to consume
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * created consumers or null to use no default configuration
	 */
	public DefaultPulsarConsumerFactory(PulsarClient pulsarClient,
			List<ConsumerBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.pulsarClient = pulsarClient;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	/**
	 * Non-fully-qualified topic names specified on the created consumers will be
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
	public Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics,
			@Nullable String subscriptionName, ConsumerBuilderCustomizer<T> customizer) {
		try {
			return createConsumer(schema, topics, subscriptionName, null,
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
	public Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics,
			@Nullable String subscriptionName, @Nullable Map<String, String> metadataProperties,
			@Nullable List<ConsumerBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		ConsumerBuilder<T> consumerBuilder = this.pulsarClient.newConsumer(schema);

		// Apply the default config customizer
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(consumerBuilder)));
		}
		// Preserve the passed in topics (don't let default config customizer win)
		if (topics != null) {
			replaceTopicsOnBuilder(consumerBuilder, topics);
		}
		if (subscriptionName != null) {
			consumerBuilder.subscriptionName(subscriptionName);
		}
		if (metadataProperties != null) {
			replaceMetadataPropertiesOnBuilder(consumerBuilder, metadataProperties);
		}
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach(customizer -> customizer.customize(consumerBuilder));
		}
		this.ensureTopicNamesFullyQualified(consumerBuilder);
		this.ensureTopicsPatternFullyQualified(consumerBuilder);
		try {
			return consumerBuilder.subscribe();
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
	}

	private void replaceTopicsOnBuilder(ConsumerBuilder<T> builder, Collection<String> topics) {
		var builderImpl = (ConsumerBuilderImpl<T>) builder;
		builderImpl.getConf().setTopicNames(new HashSet<>(topics));
	}

	private void replaceMetadataPropertiesOnBuilder(ConsumerBuilder<T> builder,
			Map<String, String> metadataProperties) {
		var builderImpl = (ConsumerBuilderImpl<T>) builder;
		builderImpl.getConf().setProperties(new TreeMap<>(metadataProperties));
	}

	protected void ensureTopicNamesFullyQualified(ConsumerBuilder<T> builder) {
		if (this.topicBuilder == null) {
			return;
		}
		var builderImpl = (ConsumerBuilderImpl<T>) builder;
		var topics = builderImpl.getConf().getTopicNames();
		if (!CollectionUtils.isEmpty(topics)) {
			var fullyQualifiedTopics = topics.stream().map(this.topicBuilder::getFullyQualifiedNameForTopic).toList();
			builderImpl.getConf().setTopicNames(new HashSet<>(fullyQualifiedTopics));
		}
	}

	protected void ensureTopicsPatternFullyQualified(ConsumerBuilder<T> builder) {
		if (this.topicBuilder == null) {
			return;
		}
		var builderImpl = (ConsumerBuilderImpl<T>) builder;
		var topicsPattern = builderImpl.getConf().getTopicsPattern();
		if (topicsPattern != null && StringUtils.isNoneBlank(topicsPattern.pattern())) {
			var topicsPatternStr = topicsPattern.pattern();
			var fullyQualifiedTopicsPatternStr = this.topicBuilder.getFullyQualifiedNameForTopic(topicsPatternStr);
			builderImpl.getConf().setTopicsPattern(Pattern.compile(fullyQualifiedTopicsPatternStr));
		}
	}

}

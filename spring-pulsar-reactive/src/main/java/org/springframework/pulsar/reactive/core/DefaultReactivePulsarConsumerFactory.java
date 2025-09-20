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
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.core.PulsarTopicBuilder;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the reactive consumer.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class DefaultReactivePulsarConsumerFactory<T> implements ReactivePulsarConsumerFactory<T> {

	private final ReactivePulsarClient reactivePulsarClient;

	private @Nullable final List<ReactiveMessageConsumerBuilderCustomizer<T>> defaultConfigCustomizers;

	private @Nullable PulsarTopicBuilder topicBuilder;

	/**
	 * Construct an instance.
	 * @param reactivePulsarClient the reactive client
	 * @param defaultConfigCustomizers the optional list of customizers that defines the
	 * default configuration for each created consumer.
	 */
	public DefaultReactivePulsarConsumerFactory(ReactivePulsarClient reactivePulsarClient,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.reactivePulsarClient = reactivePulsarClient;
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
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema) {
		return createConsumer(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers) {
		ReactiveMessageConsumerBuilder<T> consumerBuilder = this.reactivePulsarClient.messageConsumer(schema);
		// Apply the default customizers
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(consumerBuilder)));
		}
		// Apply the user specified customizers
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(consumerBuilder));
		}
		this.ensureTopicNamesFullyQualified(consumerBuilder);
		return consumerBuilder.build();
	}

	protected void ensureTopicNamesFullyQualified(ReactiveMessageConsumerBuilder<T> consumerBuilder) {
		if (this.topicBuilder == null) {
			return;
		}
		var mutableSpec = consumerBuilder.getMutableSpec();
		var topics = mutableSpec.getTopicNames();
		if (!CollectionUtils.isEmpty(topics)) {
			var fullyQualifiedTopics = topics.stream().map(this.topicBuilder::getFullyQualifiedNameForTopic).toList();
			mutableSpec.setTopicNames(fullyQualifiedTopics);
		}
		if (mutableSpec.getDeadLetterPolicy() != null) {
			var dlt = mutableSpec.getDeadLetterPolicy().getDeadLetterTopic();
			if (dlt != null) {
				mutableSpec.getDeadLetterPolicy()
					.setDeadLetterTopic(this.topicBuilder.getFullyQualifiedNameForTopic(dlt));
			}
			var rlt = mutableSpec.getDeadLetterPolicy().getRetryLetterTopic();
			if (rlt != null) {
				mutableSpec.getDeadLetterPolicy()
					.setRetryLetterTopic(this.topicBuilder.getFullyQualifiedNameForTopic(rlt));
			}
		}
	}

}

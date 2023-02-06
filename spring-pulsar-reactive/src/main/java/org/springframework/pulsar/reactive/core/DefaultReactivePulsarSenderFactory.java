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

package org.springframework.pulsar.reactive.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ImmutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link ReactivePulsarSenderFactory}.
 *
 * @param <T> reactive sender type.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class DefaultReactivePulsarSenderFactory<T> implements ReactivePulsarSenderFactory<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarClient reactivePulsarClient;

	private final ReactiveMessageSenderSpec reactiveMessageSenderSpec;

	@Nullable
	private final ReactiveMessageSenderCache reactiveMessageSenderCache;

	public DefaultReactivePulsarSenderFactory(PulsarClient pulsarClient,
			@Nullable ReactiveMessageSenderSpec reactiveMessageSenderSpec,
			@Nullable ReactiveMessageSenderCache reactiveMessageSenderCache) {
		this(AdaptedReactivePulsarClientFactory.create(pulsarClient), reactiveMessageSenderSpec,
				reactiveMessageSenderCache);
	}

	public DefaultReactivePulsarSenderFactory(ReactivePulsarClient reactivePulsarClient,
			@Nullable ReactiveMessageSenderSpec reactiveMessageSenderSpec,
			@Nullable ReactiveMessageSenderCache reactiveMessageSenderCache) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.reactiveMessageSenderSpec = new ImmutableReactiveMessageSenderSpec(
				reactiveMessageSenderSpec != null ? reactiveMessageSenderSpec : new MutableReactiveMessageSenderSpec());
		this.reactiveMessageSenderCache = reactiveMessageSenderCache;
	}

	@Override
	public ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic) {
		return doCreateReactiveMessageSender(schema, topic, null);
	}

	@Override
	public ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic,
			@Nullable ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		return doCreateReactiveMessageSender(schema, topic,
				customizer != null ? Collections.singletonList(customizer) : null);
	}

	@Override
	public ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic,
			@Nullable List<ReactiveMessageSenderBuilderCustomizer<T>> customizers) {
		return doCreateReactiveMessageSender(schema, topic, customizers);
	}

	private ReactiveMessageSender<T> doCreateReactiveMessageSender(Schema<T> schema, @Nullable String topic,
			@Nullable List<ReactiveMessageSenderBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		String resolvedTopic = ReactiveMessageSenderUtils.resolveTopicName(topic, this);
		this.logger.trace(() -> String.format("Creating reactive message sender for '%s' topic", resolvedTopic));
		ReactiveMessageSenderBuilder<T> sender = this.reactivePulsarClient.messageSender(schema);
		sender.applySpec(this.reactiveMessageSenderSpec);
		sender.topic(resolvedTopic);
		if (this.reactiveMessageSenderCache != null) {
			sender.cache(this.reactiveMessageSenderCache);
		}
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(sender));
		}
		// make sure the customizer do not override the topic
		sender.topic(resolvedTopic);

		return sender.build();
	}

	@Override
	public ReactiveMessageSenderSpec getReactiveMessageSenderSpec() {
		return this.reactiveMessageSenderSpec;
	}

}

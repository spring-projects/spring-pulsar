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

package org.springframework.pulsar.core.reactive;

import java.util.List;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link ReactivePulsarSenderFactory}.
 *
 * @param <T> reactive sender type.
 * @author Christophe Bornet
 */
public class DefaultReactivePulsarSenderFactory<T> implements ReactivePulsarSenderFactory<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarClient reactivePulsarClient;

	private final ReactiveMessageSenderSpec reactiveMessageSenderSpec;

	public DefaultReactivePulsarSenderFactory(PulsarClient pulsarClient,
			ReactiveMessageSenderSpec reactiveMessageSenderSpec) {
		this(AdaptedReactivePulsarClientFactory.create(pulsarClient), reactiveMessageSenderSpec);
	}

	public DefaultReactivePulsarSenderFactory(ReactivePulsarClient reactivePulsarClient,
			ReactiveMessageSenderSpec reactiveMessageSenderSpec) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.reactiveMessageSenderSpec = reactiveMessageSenderSpec;
	}

	@Override
	public ReactiveMessageSender<T> createReactiveMessageSender(String topic, Schema<T> schema) {
		return doCreateReactiveMessageSender(topic, schema, null, null);
	}

	@Override
	public ReactiveMessageSender<T> createReactiveMessageSender(String topic, Schema<T> schema,
			MessageRouter messageRouter) {
		return doCreateReactiveMessageSender(topic, schema, messageRouter, null);
	}

	@Override
	public ReactiveMessageSender<T> createReactiveMessageSender(String topic, Schema<T> schema,
			MessageRouter messageRouter, List<ReactiveMessageSenderBuilderCustomizer<T>> customizers) {
		return doCreateReactiveMessageSender(topic, schema, messageRouter, customizers);
	}

	private ReactiveMessageSender<T> doCreateReactiveMessageSender(String topic, Schema<T> schema,
			MessageRouter messageRouter, List<ReactiveMessageSenderBuilderCustomizer<T>> customizers) {
		final String resolvedTopic = ReactiveMessageSenderUtils.resolveTopicName(topic, this);
		this.logger.trace(() -> String.format("Creating reactive message sender for '%s' topic", resolvedTopic));
		final ReactiveMessageSenderBuilder<T> sender = this.reactivePulsarClient.messageSender(schema);
		if (this.reactiveMessageSenderSpec != null) {
			sender.applySpec(this.reactiveMessageSenderSpec);
		}
		sender.topic(resolvedTopic);
		if (messageRouter != null) {
			sender.messageRouter(messageRouter);
		}
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(sender));
		}
		return sender.build();
	}

	@Override
	public ReactiveMessageSenderSpec getReactiveMessageSenderSpec() {
		return this.reactiveMessageSenderSpec;
	}

}

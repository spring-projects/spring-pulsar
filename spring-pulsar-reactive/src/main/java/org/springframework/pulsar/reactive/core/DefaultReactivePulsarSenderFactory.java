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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link ReactivePulsarSenderFactory}.
 *
 * @param <T> underlying payload type for the reactive sender.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public final class DefaultReactivePulsarSenderFactory<T>
		implements ReactivePulsarSenderFactory<T>, RestartableComponentSupport {

	private static final int LIFECYCLE_PHASE = (Integer.MIN_VALUE / 2) - 100;

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final AtomicReference<State> currentState = RestartableComponentSupport.initialState();

	private final ReactivePulsarClient reactivePulsarClient;

	private final TopicResolver topicResolver;

	@Nullable
	private final ReactiveMessageSenderCache reactiveMessageSenderCache;

	@Nullable
	private String defaultTopic;

	@Nullable
	private final List<ReactiveMessageSenderBuilderCustomizer<T>> defaultConfigCustomizers;

	private DefaultReactivePulsarSenderFactory(ReactivePulsarClient reactivePulsarClient, TopicResolver topicResolver,
			@Nullable ReactiveMessageSenderCache reactiveMessageSenderCache, @Nullable String defaultTopic,
			@Nullable List<ReactiveMessageSenderBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.topicResolver = topicResolver;
		this.reactiveMessageSenderCache = reactiveMessageSenderCache;
		this.defaultTopic = defaultTopic;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	/**
	 * Create a builder that uses the specified Reactive pulsar client.
	 * @param reactivePulsarClient the reactive client
	 * @param <T> underlying payload type for the reactive sender
	 * @return the newly created builder instance
	 */
	public static <T> Builder<T> builderFor(ReactivePulsarClient reactivePulsarClient) {
		return new Builder<>(reactivePulsarClient);
	}

	/**
	 * Create a builder that adapts the specified pulsar client.
	 * @param pulsarClient the Pulsar client to adapt into a Reactive client
	 * @param <T> underlying payload type for the reactive sender
	 * @return the newly created builder instance
	 */
	public static <T> Builder<T> builderFor(PulsarClient pulsarClient) {
		return new Builder<>(AdaptedReactivePulsarClientFactory.create(pulsarClient));
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
		String resolvedTopic = this.topicResolver.resolveTopic(topic, () -> getDefaultTopic()).orElseThrow();
		this.logger.trace(() -> "Creating reactive message sender for '%s' topic".formatted(resolvedTopic));

		ReactiveMessageSenderBuilder<T> sender = this.reactivePulsarClient.messageSender(schema);

		// Apply the default customizers (preserve the topic)
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(sender)));
		}
		sender.topic(resolvedTopic);

		if (this.reactiveMessageSenderCache != null) {
			sender.cache(this.reactiveMessageSenderCache);
		}

		// Apply the user specified customizers (preserve the topic)
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(sender));
		}
		sender.topic(resolvedTopic);

		return sender.build();
	}

	@Override
	public String getDefaultTopic() {
		return this.defaultTopic;
	}

	/**
	 * Return the phase that this lifecycle object is supposed to run in.
	 * <p>
	 * This component has a phase that comes after the restartable client
	 * ({@code PulsarClientProxy}) but before other lifecycle and smart lifecycle
	 * components whose phase values are &quot;0&quot; and &quot;max&quot;, respectively.
	 * @return a phase that is after the restartable client and before other default
	 * components.
	 */
	@Override
	public int getPhase() {
		return LIFECYCLE_PHASE;
	}

	@Override
	public AtomicReference<State> currentState() {
		return this.currentState;
	}

	@Override
	public LogAccessor logger() {
		return this.logger;
	}

	@Override
	public void doStop() {
		try {
			this.reactiveMessageSenderCache.close();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Builder for {@link DefaultReactivePulsarSenderFactory}.
	 *
	 * @param <T> the reactive sender type
	 */
	public static final class Builder<T> {

		private final ReactivePulsarClient reactivePulsarClient;

		private TopicResolver topicResolver = new DefaultTopicResolver();

		@Nullable
		private ReactiveMessageSenderCache messageSenderCache;

		@Nullable
		private String defaultTopic;

		@Nullable
		private List<ReactiveMessageSenderBuilderCustomizer<T>> defaultConfigCustomizers;

		private Builder(ReactivePulsarClient reactivePulsarClient) {
			Assert.notNull(reactivePulsarClient, "Reactive client is required");
			this.reactivePulsarClient = reactivePulsarClient;
		}

		/**
		 * Provide the topic resolver to use.
		 * @param topicResolver the topic resolver to use
		 * @return this same builder instance
		 */
		public Builder<T> withTopicResolver(TopicResolver topicResolver) {
			this.topicResolver = topicResolver;
			return this;
		}

		/**
		 * Provide the message sender cache to use.
		 * @param messageSenderCache the message sender cache to use
		 * @return this same builder instance
		 */
		public Builder<T> withMessageSenderCache(ReactiveMessageSenderCache messageSenderCache) {
			this.messageSenderCache = messageSenderCache;
			return this;
		}

		/**
		 * Provide the default topic to use when one is not specified.
		 * @param defaultTopic the default topic to use
		 * @return this same builder instance
		 */
		public Builder<T> withDefaultTopic(String defaultTopic) {
			this.defaultTopic = defaultTopic;
			return this;
		}

		/**
		 * Provide a customizer to apply to the sender builder.
		 * @param customizer the customizer to apply to the builder before creating
		 * senders
		 * @return this same builder instance
		 */
		public Builder<T> withDefaultConfigCustomizer(ReactiveMessageSenderBuilderCustomizer<T> customizer) {
			this.defaultConfigCustomizers = List.of(customizer);
			return this;
		}

		/**
		 * Provide an optional list of sender builder customizers to apply to the builder
		 * before creating the senders.
		 * @param customizers optional list of sender builder customizers to apply to the
		 * builder before creating the senders.
		 * @return this same builder instance
		 */
		public Builder<T> withDefaultConfigCustomizers(List<ReactiveMessageSenderBuilderCustomizer<T>> customizers) {
			this.defaultConfigCustomizers = customizers;
			return this;
		}

		/**
		 * Construct the sender factory using the specified settings.
		 * @return pulsar sender factory
		 */
		public DefaultReactivePulsarSenderFactory<T> build() {
			Assert.notNull(this.topicResolver, "Topic resolver is required");
			return new DefaultReactivePulsarSenderFactory<>(this.reactivePulsarClient, this.topicResolver,
					this.messageSenderCache, this.defaultTopic, this.defaultConfigCustomizers);
		}

	}

}

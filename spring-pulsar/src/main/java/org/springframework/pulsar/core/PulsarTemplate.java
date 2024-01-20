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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarOperations.SendMessageBuilder;
import org.springframework.pulsar.observation.DefaultPulsarTemplateObservationConvention;
import org.springframework.pulsar.observation.PulsarMessageSenderContext;
import org.springframework.pulsar.observation.PulsarTemplateObservation;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;
import org.springframework.util.CollectionUtils;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * A template for executing high-level Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Jonas Geiregat
 */
public class PulsarTemplate<T>
		implements PulsarOperations<T>, ApplicationContextAware, BeanNameAware, SmartInitializingSingleton {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarProducerFactory<T> producerFactory;

	private final List<ProducerInterceptor> interceptors;

	private final SchemaResolver schemaResolver;

	private final TopicResolver topicResolver;

	/**
	 * Whether to record observations.
	 */
	private boolean observationEnabled;

	/**
	 * The registry to record observations with.
	 */
	@Nullable
	private ObservationRegistry observationRegistry;

	/**
	 * The optional custom observation convention to use when recording observations.
	 */
	@Nullable
	private PulsarTemplateObservationConvention observationConvention;

	@Nullable
	private ApplicationContext applicationContext;

	private String beanName = "";

	/**
	 * Construct a template instance without interceptors that uses the default schema
	 * resolver.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory) {
		this(producerFactory, Collections.emptyList());
	}

	/**
	 * Construct a template instance with interceptors that uses the default schema
	 * resolver and default topic resolver and enables observation recording.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 * @param interceptors the interceptors to add to the producer.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, List<ProducerInterceptor> interceptors) {
		this(producerFactory, interceptors, new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
	}

	/**
	 * Construct a template instance with optional observation configuration.
	 * @param producerFactory the factory used to create the backing Pulsar producers
	 * @param interceptors the list of interceptors to add to the producer
	 * @param schemaResolver the schema resolver to use
	 * @param topicResolver the topic resolver to use
	 * @param observationEnabled whether to record observations
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, List<ProducerInterceptor> interceptors,
			SchemaResolver schemaResolver, TopicResolver topicResolver, boolean observationEnabled) {
		this.producerFactory = producerFactory;
		this.interceptors = interceptors;
		this.schemaResolver = schemaResolver;
		this.topicResolver = topicResolver;
		this.observationEnabled = observationEnabled;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * If observations are enabled, attempt to obtain the Observation registry and
	 * convention.
	 */
	@Override
	public void afterSingletonsInstantiated() {
		if (!this.observationEnabled) {
			this.logger.debug(() -> "Observations are not enabled - not recording");
			return;
		}
		if (this.applicationContext == null) {
			this.logger.warn(() -> "Observations enabled but application context null - not recording");
			return;
		}
		this.observationRegistry = this.applicationContext.getBeanProvider(ObservationRegistry.class)
			.getIfUnique(() -> this.observationRegistry);
		this.observationConvention = this.applicationContext.getBeanProvider(PulsarTemplateObservationConvention.class)
			.getIfUnique(() -> this.observationConvention);
	}

	@Override
	public MessageId send(@Nullable T message) {
		return doSend(null, message, null, null, null, null);
	}

	@Override
	public MessageId send(@Nullable T message, @Nullable Schema<T> schema) {
		return doSend(null, message, schema, null, null, null);
	}

	@Override
	public MessageId send(@Nullable String topic, @Nullable T message) {
		return doSend(topic, message, null, null, null, null);
	}

	@Override
	public MessageId send(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema) {
		return doSend(topic, message, schema, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable T message) {
		return doSendAsync(null, message, null, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable T message, @Nullable Schema<T> schema) {
		return doSendAsync(null, message, schema, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable String topic, @Nullable T message) {
		return doSendAsync(topic, message, null, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable String topic, @Nullable T message,
			@Nullable Schema<T> schema) {
		return doSendAsync(topic, message, schema, null, null, null);
	}

	@Override
	public SendMessageBuilder<T> newMessage(@Nullable T message) {
		return new SendMessageBuilderImpl<>(this, message);
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	private MessageId doSend(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema,
			@Nullable Collection<String> encryptionKeys,
			@Nullable TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer,
			@Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		try {
			return doSendAsync(topic, message, schema, encryptionKeys, typedMessageBuilderCustomizer,
					producerCustomizer)
				.get();
		}
		catch (PulsarException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new PulsarException(PulsarClientException.unwrap(ex));
		}
	}

	private CompletableFuture<MessageId> doSendAsync(@Nullable String topic, @Nullable T message,
			@Nullable Schema<T> schema, @Nullable Collection<String> encryptionKeys,
			@Nullable TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer,
			@Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		String defaultTopic = Objects.toString(this.producerFactory.getDefaultTopic(), null);
		String topicName = this.topicResolver.resolveTopic(topic, message, () -> defaultTopic).orElseThrow();
		this.logger.trace(() -> "Sending msg to '%s' topic".formatted(topicName));

		PulsarMessageSenderContext senderContext = PulsarMessageSenderContext.newContext(topicName, this.beanName);
		Observation observation = newObservation(senderContext);
		try {
			observation.start();
			Producer<T> producer = prepareProducerForSend(topicName, message, schema, encryptionKeys,
					producerCustomizer);
			TypedMessageBuilder<T> messageBuilder;
			try {
				messageBuilder = producer.newMessage().value(message);
				if (typedMessageBuilderCustomizer != null) {
					typedMessageBuilderCustomizer.customize(messageBuilder);
				}
				// propagate props to message
				senderContext.properties().forEach(messageBuilder::property);
			}
			catch (RuntimeException ex) {
				ProducerUtils.closeProducerAsync(producer, this.logger);
				throw ex;
			}
			return messageBuilder.sendAsync().whenComplete((msgId, ex) -> {
				if (ex == null) {
					this.logger.trace(() -> "Sent msg to '%s' topic".formatted(topicName));
					observation.stop();
				}
				else {
					this.logger.error(ex, () -> "Failed to send msg to '%s' topic".formatted(topicName));
					observation.error(ex);
					observation.stop();
				}
				ProducerUtils.closeProducerAsync(producer, this.logger);
			});
		}
		catch (RuntimeException ex) {
			observation.error(ex);
			observation.stop();
			throw ex;
		}
	}

	private Observation newObservation(PulsarMessageSenderContext senderContext) {
		if (this.observationRegistry == null) {
			return Observation.NOOP;
		}
		return PulsarTemplateObservation.TEMPLATE_OBSERVATION.observation(this.observationConvention,
				DefaultPulsarTemplateObservationConvention.INSTANCE, () -> senderContext, this.observationRegistry);
	}

	private Producer<T> prepareProducerForSend(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema,
			@Nullable Collection<String> encryptionKeys, @Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		Schema<T> resolvedSchema = schema == null ? this.schemaResolver.resolveSchema(message).orElseThrow() : schema;
		List<ProducerBuilderCustomizer<T>> customizers = new ArrayList<>();
		if (!CollectionUtils.isEmpty(this.interceptors)) {
			customizers.add(builder -> this.interceptors.forEach(builder::intercept));
		}
		if (producerCustomizer != null) {
			customizers.add(producerCustomizer);
		}
		return this.producerFactory.createProducer(resolvedSchema, topic, encryptionKeys, customizers);
	}

	public static class SendMessageBuilderImpl<T> implements SendMessageBuilder<T> {

		private final PulsarTemplate<T> template;

		@Nullable
		private final T message;

		@Nullable
		private String topic;

		@Nullable
		private Schema<T> schema;

		@Nullable
		private Collection<String> encryptionKeys;

		@Nullable
		private TypedMessageBuilderCustomizer<T> messageCustomizer;

		@Nullable
		private ProducerBuilderCustomizer<T> producerCustomizer;

		SendMessageBuilderImpl(PulsarTemplate<T> template, @Nullable T message) {
			this.template = template;
			this.message = message;
		}

		@Override
		public SendMessageBuilder<T> withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withSchema(Schema<T> schema) {
			this.schema = schema;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withEncryptionKeys(Collection<String> encryptionKeys) {
			this.encryptionKeys = encryptionKeys;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withMessageCustomizer(TypedMessageBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withProducerCustomizer(ProducerBuilderCustomizer<T> producerCustomizer) {
			this.producerCustomizer = producerCustomizer;
			return this;
		}

		@Override
		public MessageId send() {
			return this.template.doSend(this.topic, this.message, this.schema, this.encryptionKeys,
					this.messageCustomizer, this.producerCustomizer);
		}

		@Override
		public CompletableFuture<MessageId> sendAsync() {
			return this.template.doSendAsync(this.topic, this.message, this.schema, this.encryptionKeys,
					this.messageCustomizer, this.producerCustomizer);
		}

	}

}

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

package org.springframework.pulsar.core;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.observation.DefaultPulsarTemplateObservationConvention;
import org.springframework.pulsar.observation.PulsarMessageSenderContext;
import org.springframework.pulsar.observation.PulsarTemplateObservation;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * A thread-safe template for executing high-level Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
public class PulsarTemplate<T> implements PulsarOperations<T>, BeanNameAware {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarProducerFactory<T> producerFactory;

	private final List<ProducerInterceptor> interceptors;

	private final ObservationRegistry observationRegistry;

	private final PulsarTemplateObservationConvention observationConvention;

	private String beanName;

	private Schema<T> schema;

	/**
	 * Construct a template instance.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory) {
		this(producerFactory, null);
	}

	/**
	 * Construct a template instance with optional interceptors.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 * @param interceptors the interceptors to add to the producer.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, List<ProducerInterceptor> interceptors) {
		this(producerFactory, interceptors, null, null);
	}

	/**
	 * Construct a template instance with optional interceptors and observation
	 * configuration.
	 * @param producerFactory the factory used to create the backing Pulsar producers
	 * @param interceptors the optional list of interceptors to add to the producer
	 * @param observationRegistry the registry to record observations with or {@code null}
	 * to not record observations
	 * @param observationConvention the optional custom observation convention to use when
	 * recording observations
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, @Nullable List<ProducerInterceptor> interceptors,
			@Nullable ObservationRegistry observationRegistry,
			@Nullable PulsarTemplateObservationConvention observationConvention) {
		this.producerFactory = producerFactory;
		this.interceptors = interceptors;
		this.observationRegistry = observationRegistry;
		this.observationConvention = observationConvention;
	}

	@Override
	public MessageId send(T message) throws PulsarClientException {
		return doSend(null, message, null, null, null);
	}

	@Override
	public MessageId send(String topic, T message) throws PulsarClientException {
		return doSend(topic, message, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(T message) throws PulsarClientException {
		return doSendAsync(null, message, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(String topic, T message) throws PulsarClientException {
		return doSendAsync(topic, message, null, null, null);
	}

	@Override
	public SendMessageBuilder<T> newMessage(T message) {
		return new SendMessageBuilderImpl<>(this, message);
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * Set the schema to use on this template.
	 * @param schema provides the {@link Schema} used on this template
	 */
	public void setSchema(Schema<T> schema) {
		this.schema = schema;
	}

	private MessageId doSend(String topic, T message, TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer,
			MessageRouter messageRouter, ProducerBuilderCustomizer<T> producerCustomizer) throws PulsarClientException {
		try {
			return doSendAsync(topic, message, typedMessageBuilderCustomizer, messageRouter, producerCustomizer).get();
		}
		catch (Exception ex) {
			throw PulsarClientException.unwrap(ex);
		}
	}

	private CompletableFuture<MessageId> doSendAsync(String topic, T message,
			TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer, MessageRouter messageRouter,
			ProducerBuilderCustomizer<T> producerCustomizer) throws PulsarClientException {
		final String topicName = ProducerUtils.resolveTopicName(topic, this.producerFactory);
		this.logger.trace(() -> String.format("Sending msg to '%s' topic", topicName));

		PulsarMessageSenderContext senderContext = PulsarMessageSenderContext.newContext(topicName, this.beanName);
		Observation observation = newObservation(senderContext);
		try {
			observation.start();
			final Producer<T> producer = prepareProducerForSend(topic, message, messageRouter, producerCustomizer);
			TypedMessageBuilder<T> messageBuilder = producer.newMessage().value(message);
			if (typedMessageBuilderCustomizer != null) {
				typedMessageBuilderCustomizer.customize(messageBuilder);
			}
			// propagate props to message
			senderContext.properties().forEach(messageBuilder::property);

			return messageBuilder.sendAsync().whenComplete((msgId, ex) -> {
				if (ex == null) {
					this.logger.trace(() -> String.format("Sent msg to '%s' topic", topicName));
					observation.stop();
				}
				else {
					this.logger.error(ex, () -> String.format("Failed to send msg to '%s' topic", topicName));
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

	private Producer<T> prepareProducerForSend(String topic, T message, MessageRouter messageRouter,
			ProducerBuilderCustomizer<T> producerCustomizer) throws PulsarClientException {
		Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		return this.producerFactory.createProducer(topic, schema, messageRouter, this.interceptors,
				producerCustomizer == null ? Collections.emptyList() : Collections.singletonList(producerCustomizer));
	}

	public static class SendMessageBuilderImpl<T> implements SendMessageBuilder<T> {

		private final PulsarTemplate<T> template;

		private final T message;

		private String topic;

		private TypedMessageBuilderCustomizer<T> messageCustomizer;

		private MessageRouter messageRouter;

		private ProducerBuilderCustomizer<T> producerCustomizer;

		SendMessageBuilderImpl(PulsarTemplate<T> template, T message) {
			this.template = template;
			this.message = message;
		}

		@Override
		public SendMessageBuilder<T> withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withMessageCustomizer(TypedMessageBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withCustomRouter(MessageRouter messageRouter) {
			this.messageRouter = messageRouter;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withProducerCustomizer(ProducerBuilderCustomizer<T> producerCustomizer) {
			this.producerCustomizer = producerCustomizer;
			return this;
		}

		@Override
		public MessageId send() throws PulsarClientException {
			return this.template.doSend(this.topic, this.message, this.messageCustomizer, this.messageRouter,
					this.producerCustomizer);
		}

		@Override
		public CompletableFuture<MessageId> sendAsync() throws PulsarClientException {
			return this.template.doSendAsync(this.topic, this.message, this.messageCustomizer, this.messageRouter,
					this.producerCustomizer);
		}

	}

}

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

import java.util.Collections;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MessageSpecBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.SchemaUtils;
import org.springframework.pulsar.observation.DefaultPulsarTemplateObservationConvention;
import org.springframework.pulsar.observation.PulsarMessageSenderContext;
import org.springframework.pulsar.observation.PulsarTemplateObservation;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import reactor.core.publisher.Mono;

/**
 * A thread-safe template for executing high-level reactive Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Christophe Bornet
 */
public class ReactivePulsarSenderTemplate<T> implements ReactivePulsarSenderOperations<T>, BeanNameAware {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory;

	private final ObservationRegistry observationRegistry;

	private final PulsarTemplateObservationConvention observationConvention;

	private String beanName;

	private Schema<T> schema;

	/**
	 * Construct a template instance.
	 * @param reactiveMessageSenderFactory the factory used to create the backing Pulsar
	 * reactive sender.
	 */
	public ReactivePulsarSenderTemplate(ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory) {
		this(reactiveMessageSenderFactory, null, null);
	}

	/**
	 * Construct a template instance with observation configuration.
	 * @param reactiveMessageSenderFactory the factory used to create the backing Pulsar
	 * reactive senders
	 * @param observationRegistry the registry to record observations with or {@code null}
	 * to not record observations
	 * @param observationConvention the optional custom observation convention to use when
	 * recording observations
	 */
	public ReactivePulsarSenderTemplate(ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory,
			@Nullable ObservationRegistry observationRegistry,
			@Nullable PulsarTemplateObservationConvention observationConvention) {
		this.reactiveMessageSenderFactory = reactiveMessageSenderFactory;
		this.observationRegistry = observationRegistry;
		this.observationConvention = observationConvention;
	}

	@Override
	public Mono<MessageId> send(T message) {
		return doSend(null, message, null, null, null);
	}

	@Override
	public Mono<MessageId> send(String topic, T message) {
		return doSend(topic, message, null, null, null);
	}

	@Override
	public ReactiveSendMessageBuilderImpl<T> newMessage(T message) {
		return new ReactiveSendMessageBuilderImpl<>(this, message);
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

	private Mono<MessageId> doSend(String topic, T message,
			MessageSpecBuilderCustomizer<T> messageSpecBuilderCustomizer, MessageRouter messageRouter,
			ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		final String topicName = ReactiveMessageSenderUtils.resolveTopicName(topic, this.reactiveMessageSenderFactory);
		this.logger.trace(() -> String.format("Sending reative msg to '%s' topic", topicName));

		PulsarMessageSenderContext senderContext = PulsarMessageSenderContext.newContext(topicName, this.beanName);
		Observation observation = newObservation(senderContext);
		try {
			observation.start();
			final ReactiveMessageSender<T> sender = createMessageSender(topic, message, messageRouter, customizer);

			MessageSpecBuilder<T> messageSpecBuilder = MessageSpec.builder(message);

			if (messageSpecBuilderCustomizer != null) {
				messageSpecBuilderCustomizer.customize(messageSpecBuilder);
			}

			// propagate props to message
			senderContext.properties().forEach(messageSpecBuilder::property);

			MessageSpec<T> messageSpec = messageSpecBuilder.build();
			return sender.sendMessage(Mono.just(messageSpec)).doOnError(ex -> {
				this.logger.error(ex, () -> String.format("Failed to send msg to '%s' topic", topicName));
				observation.error(ex);
				observation.stop();
			}).doOnSuccess(msgId -> {
				this.logger.trace(() -> String.format("Sent msg to '%s' topic", topicName));
				observation.stop();
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

	private ReactiveMessageSender<T> createMessageSender(String topic, T message, MessageRouter messageRouter,
			ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		return this.reactiveMessageSenderFactory.createReactiveMessageSender(topic, schema, messageRouter,
				customizer == null ? Collections.emptyList() : Collections.singletonList(customizer));
	}

	public static class ReactiveSendMessageBuilderImpl<T> implements ReactiveSendMessageBuilder<T> {

		private final ReactivePulsarSenderTemplate<T> template;

		private final T message;

		private String topic;

		private MessageSpecBuilderCustomizer<T> messageCustomizer;

		private MessageRouter messageRouter;

		private ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer;

		ReactiveSendMessageBuilderImpl(ReactivePulsarSenderTemplate<T> template, T message) {
			this.template = template;
			this.message = message;
		}

		@Override
		public ReactiveSendMessageBuilderImpl<T> withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		@Override
		public ReactiveSendMessageBuilderImpl<T> withMessageCustomizer(
				MessageSpecBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public ReactiveSendMessageBuilderImpl<T> withCustomRouter(MessageRouter messageRouter) {
			this.messageRouter = messageRouter;
			return this;
		}

		@Override
		public ReactiveSendMessageBuilderImpl<T> withSenderCustomizer(
				ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer) {
			this.senderCustomizer = senderCustomizer;
			return this;
		}

		@Override
		public Mono<MessageId> send() {
			return this.template.doSend(this.topic, this.message, this.messageCustomizer, this.messageRouter,
					this.senderCustomizer);
		}

	}

}

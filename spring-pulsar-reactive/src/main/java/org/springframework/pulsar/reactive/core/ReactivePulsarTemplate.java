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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageSendResult;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MessageSpecBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.reactivestreams.Publisher;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A template for executing high-level reactive Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Christophe Bornet
 */
public class ReactivePulsarTemplate<T> implements ReactivePulsarOperations<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory;

	private final SchemaResolver schemaResolver;

	private final TopicResolver topicResolver;

	/**
	 * Construct a template instance that uses the default schema resolver and topic
	 * resolver.
	 * @param reactiveMessageSenderFactory the factory used to create the backing Pulsar
	 * reactive senders
	 */
	public ReactivePulsarTemplate(ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory) {
		this(reactiveMessageSenderFactory, new DefaultSchemaResolver(), new DefaultTopicResolver());
	}

	/**
	 * Construct a template instance with a custom schema resolver and a custom topic
	 * resolver.
	 * @param reactiveMessageSenderFactory the factory used to create the backing Pulsar
	 * @param schemaResolver the schema resolver to use
	 * @param topicResolver the topic resolver to use
	 */
	public ReactivePulsarTemplate(ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory,
			SchemaResolver schemaResolver, TopicResolver topicResolver) {
		this.reactiveMessageSenderFactory = reactiveMessageSenderFactory;
		this.schemaResolver = schemaResolver;
		this.topicResolver = topicResolver;
	}

	@Override
	public Mono<MessageId> send(@Nullable T message) {
		return send(null, message);
	}

	@Override
	public Mono<MessageId> send(@Nullable T message, @Nullable Schema<T> schema) {
		return doSend(null, message, schema, null, null);
	}

	@Override
	public Mono<MessageId> send(@Nullable String topic, @Nullable T message) {
		return doSend(topic, message, null, null, null);
	}

	@Override
	public Mono<MessageId> send(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema) {
		return doSend(topic, message, schema, null, null);
	}

	@Override
	public Flux<MessageSendResult<T>> send(Publisher<MessageSpec<T>> messages) {
		return send(null, messages);
	}

	@Override
	public Flux<MessageSendResult<T>> send(Publisher<MessageSpec<T>> messages, @Nullable Schema<T> schema) {
		return doSendMany(null, Flux.from(messages), schema, null);
	}

	@Override
	public Flux<MessageSendResult<T>> send(@Nullable String topic, Publisher<MessageSpec<T>> messages) {
		return doSendMany(topic, Flux.from(messages), null, null);
	}

	@Override
	public Flux<MessageSendResult<T>> send(@Nullable String topic, Publisher<MessageSpec<T>> messages,
			@Nullable Schema<T> schema) {
		return doSendMany(topic, Flux.from(messages), schema, null);
	}

	@Override
	public SendOneMessageBuilder<T> newMessage(@Nullable T message) {
		return new SendOneMessageBuilderImpl<>(this, message);
	}

	@Override
	public SendManyMessageBuilder<T> newMessages(Publisher<MessageSpec<T>> messages) {
		return new SendManyMessageBuilderImpl<>(this, messages);
	}

	private Mono<MessageId> doSend(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema,
			@Nullable MessageSpecBuilderCustomizer<T> messageSpecBuilderCustomizer,
			@Nullable ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		String topicName = resolveTopic(topic, message);
		this.logger.trace(() -> "Sending reactive msg to '%s' topic".formatted(topicName));
		ReactiveMessageSender<T> sender = createMessageSender(topicName, message, schema, customizer);
		// @formatter:off
		return sender.sendOne(getMessageSpec(messageSpecBuilderCustomizer, message))
				.doOnError(ex -> this.logger.error(ex, () -> "Failed to send message to '%s' topic".formatted(topicName)))
				.doOnSuccess(msgId -> this.logger.trace(() -> "Sent message to '%s' topic".formatted(topicName)));
		// @formatter:on
	}

	private Flux<MessageSendResult<T>> doSendMany(@Nullable String topic, Flux<MessageSpec<T>> messages,
			@Nullable Schema<T> schema, @Nullable ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		return messages.switchOnFirst((firstSignal, messageFlux) -> {
			MessageSpec<T> firstMessage = firstSignal.get();
			if (firstMessage != null && firstSignal.isOnNext()) {
				String topicName = resolveTopic(topic, firstMessage.getValue());
				ReactiveMessageSender<T> sender = createMessageSender(topicName, firstMessage.getValue(), schema,
						customizer);
				return messageFlux.as(sender::sendMany)
					.doOnError(ex -> this.logger.error(ex,
							() -> "Failed to send messages to '%s' topic".formatted(topicName)))
					.doOnNext(msgId -> this.logger.trace(() -> "Sent messages to '%s' topic".formatted(topicName)));
			}
			// The flux has errored or is completed
			return messageFlux.thenMany(Flux.empty());
		});
	}

	private String resolveTopic(@Nullable String topic, @Nullable Object message) {
		String defaultTopic = this.reactiveMessageSenderFactory.getDefaultTopic();
		return this.topicResolver.resolveTopic(topic, message, () -> defaultTopic).orElseThrow();
	}

	private static <T> MessageSpec<T> getMessageSpec(
			@Nullable MessageSpecBuilderCustomizer<T> messageSpecBuilderCustomizer, @Nullable T message) {
		MessageSpecBuilder<T> messageSpecBuilder = MessageSpec.builder(message);
		if (messageSpecBuilderCustomizer != null) {
			messageSpecBuilderCustomizer.customize(messageSpecBuilder);
		}
		return messageSpecBuilder.build();
	}

	private ReactiveMessageSender<T> createMessageSender(@Nullable String topic, @Nullable T message,
			@Nullable Schema<T> schema, @Nullable ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		Schema<T> resolvedSchema = schema == null ? this.schemaResolver.resolveSchema(message).orElseThrow() : schema;
		return this.reactiveMessageSenderFactory.createSender(resolvedSchema, topic, customizer);
	}

	private static class SendMessageBuilderImpl<O, T> {

		protected final ReactivePulsarTemplate<T> template;

		@Nullable
		protected String topic;

		@Nullable
		protected Schema<T> schema;

		@Nullable
		protected ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer;

		SendMessageBuilderImpl(ReactivePulsarTemplate<T> template) {
			this.template = template;
		}

		@SuppressWarnings("unchecked")
		public O withTopic(String topic) {
			this.topic = topic;
			return (O) this;
		}

		@SuppressWarnings("unchecked")
		public O withSchema(Schema<T> schema) {
			this.schema = schema;
			return (O) this;
		}

		@SuppressWarnings("unchecked")
		public O withSenderCustomizer(ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer) {
			this.senderCustomizer = senderCustomizer;
			return (O) this;
		}

	}

	private static final class SendOneMessageBuilderImpl<T>
			extends SendMessageBuilderImpl<SendOneMessageBuilderImpl<T>, T> implements SendOneMessageBuilder<T> {

		@Nullable
		private final T message;

		@Nullable
		private MessageSpecBuilderCustomizer<T> messageCustomizer;

		SendOneMessageBuilderImpl(ReactivePulsarTemplate<T> template, @Nullable T message) {
			super(template);
			this.message = message;
		}

		@Override
		public SendOneMessageBuilderImpl<T> withMessageCustomizer(MessageSpecBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public Mono<MessageId> send() {
			return this.template.doSend(this.topic, this.message, this.schema, this.messageCustomizer,
					this.senderCustomizer);
		}

	}

	private static final class SendManyMessageBuilderImpl<T>
			extends SendMessageBuilderImpl<SendManyMessageBuilderImpl<T>, T> implements SendManyMessageBuilder<T> {

		private final Publisher<MessageSpec<T>> messages;

		SendManyMessageBuilderImpl(ReactivePulsarTemplate<T> template, Publisher<MessageSpec<T>> messages) {
			super(template);
			this.messages = messages;
		}

		@Override
		public Flux<MessageSendResult<T>> send() {
			return this.template.doSendMany(this.topic, Flux.from(this.messages), this.schema, this.senderCustomizer);
		}

	}

}

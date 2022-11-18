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

package org.springframework.pulsar.reactive.core;

import java.util.Collections;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.MessageSpecBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.reactivestreams.Publisher;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.SchemaUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A thread-safe template for executing high-level reactive Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Christophe Bornet
 */
public class ReactivePulsarTemplate<T> implements ReactivePulsarOperations<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory;

	private Schema<T> schema;

	/**
	 * Construct a template instance with observation configuration.
	 * @param reactiveMessageSenderFactory the factory used to create the backing Pulsar
	 * reactive senders
	 */
	public ReactivePulsarTemplate(ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory) {
		this.reactiveMessageSenderFactory = reactiveMessageSenderFactory;
	}

	@Override
	public Mono<MessageId> send(T message) {
		return send(null, message);
	}

	@Override
	public Mono<MessageId> send(String topic, T message) {
		return doSend(topic, message, null, null);
	}

	@Override
	public Flux<MessageId> send(Publisher<T> messages) {
		return send(null, messages);
	}

	@Override
	public Flux<MessageId> send(String topic, Publisher<T> messages) {
		return doSendMany(topic, Flux.from(messages));
	}

	@Override
	public SendMessageBuilderImpl<T> newMessage(T message) {
		return new SendMessageBuilderImpl<>(this, message);
	}

	/**
	 * Set the schema to use on this template.
	 * @param schema provides the {@link Schema} used on this template
	 */
	public void setSchema(Schema<T> schema) {
		this.schema = schema;
	}

	private Mono<MessageId> doSend(String topic, T message,
			MessageSpecBuilderCustomizer<T> messageSpecBuilderCustomizer,
			ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		String topicName = ReactiveMessageSenderUtils.resolveTopicName(topic, this.reactiveMessageSenderFactory);
		this.logger.trace(() -> String.format("Sending reactive message to '%s' topic", topicName));
		ReactiveMessageSender<T> sender = createMessageSender(topic, message, customizer);
		return sender.sendOne(getMessageSpec(messageSpecBuilderCustomizer, message)).doOnError(
				ex -> this.logger.error(ex, () -> String.format("Failed to send message to '%s' topic", topicName)))
				.doOnSuccess(msgId -> this.logger.trace(() -> String.format("Sent message to '%s' topic", topicName)));

	}

	private Flux<MessageId> doSendMany(String topic, Flux<T> messages) {
		String topicName = ReactiveMessageSenderUtils.resolveTopicName(topic, this.reactiveMessageSenderFactory);
		this.logger.trace(() -> String.format("Sending reactive messages to '%s' topic", topicName));

		if (this.schema != null) {
			/*
			 * If the template has a schema, we can create the message sender right away
			 * and use ReactiveMessageSender::sendMany to send them as a stream. Otherwise
			 * we need to wait to get a message to create it and we can't share it between
			 * messages. So we create one each time and use ReactiveMessageSender::sendOne
			 * to send messages individually.
			 */
			ReactiveMessageSender<T> sender = createMessageSender(topic, null, null);
			return messages.map(MessageSpec::of).as(sender::sendMany)
					.doOnError(ex -> this.logger.error(ex,
							() -> String.format("Failed to send messages to '%s' topic", topicName)))
					.doOnNext(
							msgId -> this.logger.trace(() -> String.format("Sent messages to '%s' topic", topicName)));
		}
		return messages.flatMapSequential(message -> doSend(topic, message, null, null));
	}

	private static <T> MessageSpec<T> getMessageSpec(MessageSpecBuilderCustomizer<T> messageSpecBuilderCustomizer,
			T message) {
		MessageSpecBuilder<T> messageSpecBuilder = MessageSpec.builder(message);

		if (messageSpecBuilderCustomizer != null) {
			messageSpecBuilderCustomizer.customize(messageSpecBuilder);
		}

		return messageSpecBuilder.build();
	}

	private ReactiveMessageSender<T> createMessageSender(String topic, T message,
			ReactiveMessageSenderBuilderCustomizer<T> customizer) {
		Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		return this.reactiveMessageSenderFactory.createSender(topic, schema,
				customizer == null ? Collections.emptyList() : Collections.singletonList(customizer));
	}

	public static class SendMessageBuilderImpl<T> implements SendMessageBuilder<T> {

		private final ReactivePulsarTemplate<T> template;

		private final T message;

		private String topic;

		private MessageSpecBuilderCustomizer<T> messageCustomizer;

		private ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer;

		SendMessageBuilderImpl(ReactivePulsarTemplate<T> template, T message) {
			this.template = template;
			this.message = message;
		}

		@Override
		public SendMessageBuilderImpl<T> withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		@Override
		public SendMessageBuilderImpl<T> withMessageCustomizer(MessageSpecBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public SendMessageBuilderImpl<T> withSenderCustomizer(
				ReactiveMessageSenderBuilderCustomizer<T> senderCustomizer) {
			this.senderCustomizer = senderCustomizer;
			return this;
		}

		@Override
		public Mono<MessageId> send() {
			return this.template.doSend(this.topic, this.message, this.messageCustomizer, this.senderCustomizer);
		}

	}

}

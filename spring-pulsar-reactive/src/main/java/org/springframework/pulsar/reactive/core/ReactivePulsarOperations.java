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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.reactivestreams.Publisher;

import org.springframework.lang.Nullable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The Pulsar reactive send operations contract.
 *
 * @param <T> the message payload type
 * @author Christophe Bornet
 * @author Chris Bono
 */
public interface ReactivePulsarOperations<T> {

	/**
	 * Sends a message to the default topic in a reactive manner.
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(T message);

	/**
	 * Sends a message to the specified topic in a reactive manner. default topic
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to use the default schema
	 * resolution
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(T message, @Nullable Schema<T> schema);

	/**
	 * Sends a message to the specified topic in a reactive manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(@Nullable String topic, T message);

	/**
	 * Sends a message to the specified topic in a reactive manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to use the default schema
	 * resolution
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(@Nullable String topic, T message, @Nullable Schema<T> schema);

	/**
	 * Sends multiple messages to the default topic in a reactive manner.
	 * @param messages the messages to send
	 * @return the ids assigned by the broker to the published messages in the same order
	 * as they were sent
	 */
	Flux<MessageId> send(Publisher<T> messages);

	/**
	 * Sends multiple messages to the default topic in a reactive manner.
	 * @param messages the messages to send
	 * @param schema the schema to use or {@code null} to use the default schema
	 * resolution
	 * @return the ids assigned by the broker to the published messages in the same order
	 * as they were sent
	 */
	Flux<MessageId> send(Publisher<T> messages, @Nullable Schema<T> schema);

	/**
	 * Sends multiple messages to the specified topic in a reactive manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param messages the messages to send
	 * @return the ids assigned by the broker to the published messages in the same order
	 * as they were sent
	 */
	Flux<MessageId> send(@Nullable String topic, Publisher<T> messages);

	/**
	 * Sends multiple messages to the specified topic in a reactive manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param messages the messages to send
	 * @param schema the schema to use or {@code null} to use the default schema
	 * resolution
	 * @return the ids assigned by the broker to the published messages in the same order
	 * as they were sent
	 */
	Flux<MessageId> send(@Nullable String topic, Publisher<T> messages, @Nullable Schema<T> schema);

	/**
	 * Create a {@link SendOneMessageBuilder builder} for configuring and sending a
	 * message reactively.
	 * @param message the payload of the message
	 * @return the builder to configure and send the message
	 */
	SendOneMessageBuilder<T> newMessage(T message);

	/**
	 * Create a {@link SendManyMessageBuilder builder} for configuring and sending
	 * multiple messages reactively.
	 * @param messages the messages to send
	 * @return the builder to configure and send the message
	 */
	SendManyMessageBuilder<T> newMessages(Publisher<T> messages);

	/**
	 * Builder that can be used to configure and send a message. Provides more options
	 * than the send methods provided by {@link ReactivePulsarOperations}.
	 *
	 * @param <O> the builder type
	 * @param <T> the message payload type
	 */
	sealed interface SendMessageBuilder<O, T> permits SendOneMessageBuilder, SendManyMessageBuilder {

		/**
		 * Specify the topic to send the message to.
		 * @param topic the destination topic
		 * @return the current builder with the destination topic specified
		 */
		O withTopic(String topic);

		/**
		 * Specify the schema to use when sending the message.
		 * @param schema the schema to use
		 * @return the current builder with the schema specified
		 */
		O withSchema(Schema<T> schema);

		/**
		 * Specifies the customizer to use to further configure the reactive sender
		 * builder.
		 * @param customizer the reactive sender builder customizer
		 * @return the current builder with the reactive sender builder customizer
		 * specified
		 */
		O withSenderCustomizer(ReactiveMessageSenderBuilderCustomizer<T> customizer);

	}

	non-sealed interface SendOneMessageBuilder<T> extends SendMessageBuilder<SendOneMessageBuilder<T>, T> {

		/**
		 * Specifies the message customizer to use to further configure the message.
		 * @param customizer the message customizer
		 * @return the current builder with the message customizer specified
		 */
		SendOneMessageBuilder<T> withMessageCustomizer(MessageSpecBuilderCustomizer<T> customizer);

		/**
		 * Send the message in a reactive manner using the configured specification.
		 * @return the id assigned by the broker to the published message
		 */
		Mono<MessageId> send();

	}

	non-sealed interface SendManyMessageBuilder<T> extends SendMessageBuilder<SendManyMessageBuilder<T>, T> {

		/**
		 * Send the messages in a reactive manner using the configured specification.
		 * @return the ids assigned by the broker to the published messages in the same
		 * order as they were sent
		 */
		Flux<MessageId> send();

	}

}

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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;

import reactor.core.publisher.Mono;

/**
 * The Pulsar reactive send operations contract.
 *
 * @param <T> the message payload type
 * @author Christophe Bornet
 */
public interface ReactivePulsarSenderOperations<T> {

	/**
	 * Sends a message to the default topic in a reactive manner.
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(T message);

	/**
	 * Sends a message to the specified topic in a reactive manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 */
	Mono<MessageId> send(String topic, T message);

	/**
	 * Create a {@link SendMessageBuilder builder} for configuring and sending a message
	 * reactively.
	 * @param message the payload of the message
	 * @return the builder to configure and send the message
	 */
	SendMessageBuilder<T> newMessage(T message);

	/**
	 * Builder that can be used to configure and send a message. Provides more options
	 * than the send methods provided by {@link ReactivePulsarSenderOperations}.
	 *
	 * @param <T> the message payload type
	 */
	interface SendMessageBuilder<T> {

		/**
		 * Specify the topic to send the message to.
		 * @param topic the destination topic
		 * @return the current builder with the destination topic specified
		 */
		SendMessageBuilder<T> withTopic(String topic);

		/**
		 * Specifies the message customizer to use to further configure the message.
		 * @param customizer the message customizer
		 * @return the current builder with the message customizer specified
		 */
		SendMessageBuilder<T> withMessageCustomizer(MessageSpecBuilderCustomizer<T> customizer);

		/**
		 * Specifies the custom message router to use when sending the message.
		 * @param messageRouter the custom message router
		 * @return the current builder with the custom message router specified
		 */
		SendMessageBuilder<T> withCustomRouter(MessageRouter messageRouter);

		/**
		 * Specifies the customizer to use to further configure the reactive sender
		 * builder.
		 * @param customizer the reactive sender builder customizer
		 * @return the current builder with the reactive sender builder customizer
		 * specified
		 */
		SendMessageBuilder<T> withSenderCustomizer(ReactiveMessageSenderBuilderCustomizer<T> customizer);

		/**
		 * Send the message in a reactive manner using the configured specification.
		 * @return the id assigned by the broker to the published message
		 */
		Mono<MessageId> send();

	}

}

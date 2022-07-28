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

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * The basic Pulsar operations contract.
 *
 * @param <T> the message payload type
 *
 * @author Chris Bono
 */
public interface PulsarOperations<T> {

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @return the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default MessageId send(T message) throws PulsarClientException {
		return send(null, message, null);
	}

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @return the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default MessageId send(String topic, T message) throws PulsarClientException {
		return send(topic, message, null);
	}

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @return the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default MessageId send(T message, MessageRouter messageRouter) throws PulsarClientException {
		return send(null, message, messageRouter);
	}

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @return the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default MessageId send(String topic, T message, MessageRouter messageRouter) throws PulsarClientException {
		return send(topic, message, messageRouter, null);
	}

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @param typedMessageBuilderCustomizer the optional TypedMessageBuilder customizer
	 * @return the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	MessageId send(String topic, T message, MessageRouter messageRouter, TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer) throws PulsarClientException;

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @return a future that holds the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default CompletableFuture<MessageId> sendAsync(T message) throws PulsarClientException {
		return sendAsync(null, message, null);
	}

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @return a future that holds the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default CompletableFuture<MessageId> sendAsync(String topic, T message) throws PulsarClientException {
		return sendAsync(topic, message, null);
	}

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @return a future that holds the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default CompletableFuture<MessageId> sendAsync(T message, MessageRouter messageRouter) throws PulsarClientException {
		return sendAsync(null, message, messageRouter);
	}

	/**
	 * Sends a message to the specified topic in a non-blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @return a future that holds the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	default CompletableFuture<MessageId> sendAsync(String topic, T message, MessageRouter messageRouter) throws PulsarClientException {
		return sendAsync(topic, message, messageRouter, null);
	}

	/**
	 * Sends a message to the specified topic in a non-blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the default topic
	 * @param message the message to send
	 * @param messageRouter the optional message router to use
	 * @param typedMessageBuilderCustomizer the optional TypedMessageBuilder customizer
	 * @return a future that holds the id of the sent message
	 * @throws PulsarClientException if an error occurs
	 */
	CompletableFuture<MessageId> sendAsync(String topic, T message, MessageRouter messageRouter, TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer) throws PulsarClientException;
}

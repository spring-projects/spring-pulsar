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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;

/**
 * The basic Pulsar operations contract.
 *
 * @param <T> the message payload type
 * @author Chris Bono
 * @author Alexander Preuß
 */
public interface PulsarOperations<T> {

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	MessageId send(T message) throws PulsarClientException;

	/**
	 * Sends a message to the default topic in a blocking manner.
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to send using the default schema
	 * resolution
	 * @return the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	MessageId send(T message, @Nullable Schema<T> schema) throws PulsarClientException;

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @return the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	MessageId send(@Nullable String topic, T message) throws PulsarClientException;

	/**
	 * Sends a message to the specified topic in a blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to send using the default schema
	 * resolution
	 * @return the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	MessageId send(@Nullable String topic, T message, @Nullable Schema<T> schema) throws PulsarClientException;

	/**
	 * Sends a message to the default topic in a non-blocking manner.
	 * @param message the message to send
	 * @return a future that holds the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	CompletableFuture<MessageId> sendAsync(T message) throws PulsarClientException;

	/**
	 * Sends a message to the default topic in a non-blocking manner.
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to send using the default schema
	 * resolution
	 * @return a future that holds the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	CompletableFuture<MessageId> sendAsync(T message, @Nullable Schema<T> schema) throws PulsarClientException;

	/**
	 * Sends a message to the specified topic in a non-blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @return a future that holds the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	CompletableFuture<MessageId> sendAsync(@Nullable String topic, T message) throws PulsarClientException;

	/**
	 * Sends a message to the specified topic in a non-blocking manner.
	 * @param topic the topic to send the message to or {@code null} to send to the
	 * default topic
	 * @param message the message to send
	 * @param schema the schema to use or {@code null} to send using the default schema
	 * resolution
	 * @return a future that holds the id assigned by the broker to the published message
	 * @throws PulsarClientException if an error occurs
	 */
	CompletableFuture<MessageId> sendAsync(@Nullable String topic, T message, @Nullable Schema<T> schema)
			throws PulsarClientException;

	/**
	 * Create a {@link SendMessageBuilder builder} for configuring and sending a message.
	 * @param message the payload of the message
	 * @return the builder to configure and send the message
	 */
	SendMessageBuilder<T> newMessage(T message);

	/**
	 * Builder that can be used to configure and send a message. Provides more options
	 * than the basic send/sendAsync methods provided by {@link PulsarOperations}.
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
		 * Specify the schema to use when sending the message.
		 * @param schema the schema to use
		 * @return the current builder with the schema specified
		 */
		SendMessageBuilder<T> withSchema(Schema<T> schema);

		/**
		 * Specify the encryption keys to use.
		 * @param encryptionKeys the encryption keys
		 * @return the current builder with the encryption keys specified
		 */
		SendMessageBuilder<T> withEncryptionKeys(Collection<String> encryptionKeys);

		/**
		 * Specifies the message customizer to use to further configure the message.
		 * @param messageCustomizer the message customizer
		 * @return the current builder with the message customizer specified
		 */
		SendMessageBuilder<T> withMessageCustomizer(TypedMessageBuilderCustomizer<T> messageCustomizer);

		/**
		 * Specifies the customizer to use to further configure the producer builder.
		 * @param producerCustomizer the producer builder customizer
		 * @return the current builder with the producer builder customizer specified
		 */
		SendMessageBuilder<T> withProducerCustomizer(ProducerBuilderCustomizer<T> producerCustomizer);

		/**
		 * Send the message in a blocking manner using the configured specification.
		 * @return the id assigned by the broker to the published message
		 * @throws PulsarClientException if an error occurs
		 */
		MessageId send() throws PulsarClientException;

		/**
		 * Uses the configured specification to send the message in a non-blocking manner.
		 * @return a future that holds the id assigned by the broker to the published
		 * message
		 * @throws PulsarClientException if an error occurs
		 */
		CompletableFuture<MessageId> sendAsync() throws PulsarClientException;

	}

}

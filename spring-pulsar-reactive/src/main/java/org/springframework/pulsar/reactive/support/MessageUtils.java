/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.pulsar.reactive.support;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.reactive.client.api.MessageResult;

import org.springframework.messaging.Message;
import org.springframework.pulsar.support.PulsarHeaders;

/**
 * Convenience functions related to Spring {@link Message messages}.
 *
 * @author Chris Bono
 */
public final class MessageUtils {

	private MessageUtils() {
	}

	/**
	 * Determine the Pulsar {@link MessageId} for a given Spring message by extracting the
	 * value of its {@link PulsarHeaders#MESSAGE_ID} header.
	 * @param <T> the type of message payload
	 * @param message the Spring message
	 * @return the Pulsar message id
	 * @throws IllegalStateException if the message id could not be determined
	 */
	public static <T> MessageId extractMessageId(Message<T> message) {
		if (message.getHeaders().get(PulsarHeaders.MESSAGE_ID) instanceof MessageId msgId) {
			return msgId;
		}
		throw new IllegalStateException("Spring Message missing '%s' header".formatted(PulsarHeaders.MESSAGE_ID));
	}

	/**
	 * Convenience method that acknowledges a Spring message by {@link #extractMessageId
	 * extracting} its message id and passing it to
	 * {@link MessageResult#acknowledge(MessageId)}.
	 * @param <T> the type of message payload
	 * @param message the Spring message to acknowledge
	 * @return an empty value and signals that the message must be acknowledged
	 */
	public static <T> MessageResult<Void> acknowledge(Message<T> message) {
		return MessageResult.acknowledge(MessageUtils.extractMessageId(message));
	}

	/**
	 * Convenience method that negatively acknowledges a Spring message by
	 * {@link #extractMessageId extracting} its message id and passing it to
	 * {@link MessageResult#negativeAcknowledge(MessageId)}.
	 * @param <T> the type of message payload
	 * @param message the Spring message to negatively acknowledge
	 * @return an empty value and signals that the message must be negatively acknowledged
	 */
	public static <T> MessageResult<Void> negativeAcknowledge(Message<T> message) {
		return MessageResult.negativeAcknowledge(MessageUtils.extractMessageId(message));
	}

}

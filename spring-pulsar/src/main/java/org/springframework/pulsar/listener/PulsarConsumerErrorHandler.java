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

package org.springframework.pulsar.listener;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 * Spring provided error handler for tackling errors from Pulsar consumer. When using this
 * mechanism, make sure not to set ackTimeoutMillis as a property.
 *
 * Contract for consumer error handling through the message listener container. Both
 * record and batch message listener errors are handled through this interface.
 *
 * When an implementation for this interface is provided to the message listener
 * container, then if the listener method throws an exception, errors will be handled
 * through it.
 *
 * @param <T> payload type managed by the consumer
 * @author Soby Chacko
 */
public interface PulsarConsumerErrorHandler<T> {

	/**
	 * Decide if the failed message should be retried.
	 * @param thrownException throws exception
	 * @param message Pulsar message
	 * @return if the failed message should be retried or not
	 */
	boolean shouldRetryMessageInError(Exception thrownException, Message<T> message);

	/**
	 * Recover the message based on the implementation provided. Once this method returns,
	 * callers can assume that the message is recovered and can acknowledge the message.
	 * @param consumer Pulsar consumer
	 * @param message Pulsar message
	 * @param thrownException thrown exception
	 */
	void recoverMessage(Consumer<T> consumer, Message<T> message, Exception thrownException);

	/**
	 * Returns the Pulsar message that is tracked by the error handler.
	 * @return the Pulsar Message currently tracked by the error handler
	 */
	Message<T> getTheCurrentPulsarMessageTracked();

	/**
	 * Clears the thread state managed by this error handler.
	 */
	void clearThreadState();

}

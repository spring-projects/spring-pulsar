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

package org.springframework.pulsar.listener;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.jspecify.annotations.Nullable;

/**
 *
 * Contract for consumer error handling through the message listener container. Both
 * record and batch message listener errors are handled through this interface.
 *
 * When an error handler implementation is provided to the message listener container, the
 * container will funnel all errors through it for handling.
 *
 * @param <T> payload type managed by the consumer
 * @author Soby Chacko
 */
public interface PulsarConsumerErrorHandler<T> {

	/**
	 * Decide if the failed message should be retried.
	 * @param exception throws exception
	 * @param message Pulsar message
	 * @return if the failed message should be retried or not
	 */
	boolean shouldRetryMessage(Exception exception, Message<T> message);

	/**
	 * Recover the message based on the implementation provided. Once this method returns,
	 * callers can assume that the message is recovered and has not been acknowledged yet.
	 * @param consumer Pulsar consumer
	 * @param message Pulsar message
	 * @param thrownException thrown exception
	 */
	void recoverMessage(Consumer<T> consumer, Message<T> message, Exception thrownException);

	/**
	 * Returns the current message in error.
	 * @return the Pulsar Message currently tracked by the error handler or null if none
	 * currently tracked
	 */
	@Nullable Message<T> currentMessage();

	/**
	 * Clear the message in error from managing (such as resetting any thread state etc.).
	 */
	void clearMessage();

}

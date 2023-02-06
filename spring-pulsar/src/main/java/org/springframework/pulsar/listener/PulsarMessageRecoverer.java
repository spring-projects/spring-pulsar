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

package org.springframework.pulsar.listener;

import java.util.function.BiConsumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 * Allows recovering a failed Pulsar message.
 *
 * Implementations can choose how the message needs to be recovered by providing a
 * {@link java.util.function.Function} implementation that takes a {@link Consumer} and
 * then provide a {@link BiConsumer} which takes {@link Message} and the thrown
 * {@link Exception}.
 *
 * @param <T> payload type of Pulsar message.
 * @author Soby Chacko
 * @author Chris Bono
 */
@FunctionalInterface
public interface PulsarMessageRecoverer<T> {

	/**
	 * Recover a failed message, for e.g. send the message to a DLT.
	 * @param message Pulsar message
	 * @param exception exception from failed message
	 */
	void recoverMessage(Message<T> message, Exception exception);

}

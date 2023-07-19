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

import java.util.function.BiFunction;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.PulsarOperations;

/**
 * {@link PulsarMessageRecoverer} implementation that is capable of recovering the message
 * by publishing the failed record to a DLT - Dead Letter Topic.
 *
 * @param <T> payload type of the Pulsar message
 * @author Soby Chacko
 */
public class PulsarDeadLetterPublishingRecoverer<T> implements PulsarMessageRecovererFactory<T> {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	/**
	 * TODO: Move this to a common constants class.
	 *
	 * exception cause for the failed message.
	 */
	public static final String EXCEPTION_THROWN_CAUSE = "exception-thrown-cause";

	private static final BiFunction<Consumer<?>, Message<?>, String> DEFAULT_DESTINATION_RESOLVER = (c,
			m) -> m.getTopicName() + "-" + c.getSubscription() + "-DLT";

	private final PulsarOperations<T> pulsarTemplate;

	private final BiFunction<Consumer<?>, Message<?>, String> destinationResolver;

	public PulsarDeadLetterPublishingRecoverer(PulsarOperations<T> pulsarTemplate) {
		this(pulsarTemplate, DEFAULT_DESTINATION_RESOLVER);
	}

	public PulsarDeadLetterPublishingRecoverer(PulsarOperations<T> pulsarTemplate,
			BiFunction<Consumer<?>, Message<?>, String> destinationResolver) {
		this.pulsarTemplate = pulsarTemplate;
		this.destinationResolver = destinationResolver;
	}

	@Override
	public PulsarMessageRecoverer<T> recovererForConsumer(Consumer<T> consumer) {
		return (message, exception) -> {
			try {
				this.pulsarTemplate.newMessage(message.getValue())
					.withTopic(this.destinationResolver.apply(consumer, message))
					.withMessageCustomizer(messageBuilder -> messageBuilder.property(EXCEPTION_THROWN_CAUSE,
							exception.getCause() != null ? exception.getCause().getMessage() : exception.getMessage()))
					.sendAsync();
			}
			catch (PulsarClientException e) {
				this.logger.error(e, "DLT publishing failed.");
			}
		};
	}

}

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

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import org.springframework.core.log.LogAccessor;

/**
 * A thread-safe template for executing high-level Pulsar operations.
 *
 * @param <T> the message payload type
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
public class PulsarTemplate<T> implements PulsarOperations<T> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final PulsarProducerFactory<T> producerFactory;

	/**
	 * Constructs a template instance.
	 * @param producerFactory the producer factory used to create the backing Pulsar producers.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory) {
		this.producerFactory = producerFactory;
	}

	@Override
	public MessageId send(String topic, T message, TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer, MessageRouter messageRouter) throws PulsarClientException {
		try {
			return this.sendAsync(topic, message, typedMessageBuilderCustomizer, messageRouter).get();
		}
		catch (Exception ex) {
			throw PulsarClientException.unwrap(ex);
		}
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(String topic, T message, TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer, MessageRouter messageRouter) throws PulsarClientException {
		final String topicName = ProducerUtils.resolveTopicName(topic, this.producerFactory);
		this.logger.trace(() -> String.format("Sending msg to '%s' topic", topicName));
		final Producer<T> producer = prepareProducerForSend(topic, message, messageRouter);
		TypedMessageBuilder<T> messageBuilder = producer.newMessage().value(message);
		if (typedMessageBuilderCustomizer != null) {
			typedMessageBuilderCustomizer.customize(messageBuilder);
		}
		return messageBuilder.sendAsync()
				.whenComplete((msgId, ex) -> {
					if (ex == null) {
						this.logger.trace(() -> String.format("Sent msg to '%s' topic", topicName));
						// TODO success metrics
					}
					else {
						this.logger.error(ex, () -> String.format("Failed to send msg to '%s' topic", topicName));
						// TODO fail metrics
					}
					ProducerUtils.closeProducerAsync(producer, this.logger);
				});
	}

	private Producer<T> prepareProducerForSend(String topic, T message, MessageRouter messageRouter) throws PulsarClientException {
		Schema<T> schema = SchemaUtils.getSchema(message);
		return this.producerFactory.createProducer(topic, schema, messageRouter);
	}
}

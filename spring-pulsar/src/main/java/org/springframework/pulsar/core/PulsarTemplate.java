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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.StringUtils;

/**
 * A thread-safe template for executing high-level Pulsar operations.
 *
 * @param <T> the message payload type
 *
 * @author Soby Chacko
 * @author Chris Bono
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
	public MessageId send(String topic, T message) throws PulsarClientException {
		try {
			return this.sendAsync(topic, message).get();
		}
		catch (Exception ex) {
			throw PulsarClientException.unwrap(ex);
		}
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(String topic, T message, MessageRouter messageRouter) throws PulsarClientException {
		final String topicName = resolveTopicName(topic);
		this.logger.trace(() -> String.format("Sending msg to '%s' topic", topicName));
		final Producer<T> producer = prepareProducerForSend(topic, message, messageRouter);
		return producer.sendAsync(message)
				.whenComplete((msgId, ex) -> {
					if (ex == null) {
						this.logger.trace(() -> String.format("Sent msg to '%s' topic", topicName));
						// TODO success metrics
					}
					else {
						this.logger.error(ex, () -> String.format("Failed to send msg to '%s' topic", topicName));
						// TODO fail metrics
					}
					closeProducerAsync(producer);
				});
	}

	private String resolveTopicName(String userSpecifiedTopic) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return userSpecifiedTopic;
		}
		return Optional.ofNullable(this.producerFactory.getProducerConfig().get("topicName"))
				.map(Object::toString)
				.orElseThrow(() -> new IllegalArgumentException("Topic must be specified when no default topic is configured"));
	}

	private Producer<T> prepareProducerForSend(String topic, T message, MessageRouter messageRouter) throws PulsarClientException {
		Schema<T> schema = SchemaUtils.getSchema(message);
		return this.producerFactory.createProducer(topic, schema, messageRouter);
	}

	private void closeProducerAsync(Producer<T> producer) {
		producer.closeAsync().exceptionally(e -> {
			this.logger.warn(e, () -> String.format("Failed to close producer %s:%s", producer.getProducerName(), producer.getTopic()));
			return null;
		});
	}
}

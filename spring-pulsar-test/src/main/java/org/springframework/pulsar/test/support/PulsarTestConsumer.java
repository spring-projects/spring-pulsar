/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.pulsar.test.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.awaitility.core.ConditionTimeoutException;

import org.springframework.pulsar.core.PulsarConsumerFactory;

/**
 * Fluent API, to be used in tests, for consuming messages from Pulsar topics until a
 * certain {@code Condition} has been met.
 *
 * @param <T> the type of the message payload
 * @author Jonas Geiregat
 */
public class PulsarTestConsumer<T> implements TopicStep, SchemaStep, ConditionsStep<T> {

	private final PulsarConsumerFactory<T> consumerFactory;

	private List<String> topics;

	private Condition<T> condition;

	private Schema<T> schema;

	private Duration timeout = Duration.ofSeconds(30);

	public static <T> TopicStep consumeMessages(PulsarConsumerFactory<T> pulsarConsumerFactory) {
		return new PulsarTestConsumer<>(pulsarConsumerFactory);
	}

	public PulsarTestConsumer(PulsarConsumerFactory<T> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	@Override
	public PulsarTestConsumer<T> fromTopic(String... topics) {
		this.topics = List.of(topics);
		return this;
	}

	@Override
	public ConditionsStep<T> awaitAtMost(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	@Override
	public List<Message<T>> until(Condition<T> condition) {
		this.condition = condition;
		try {
			return doGet();
		}
		catch (ConditionTimeoutException ex) {
			throw new PulsarTimeOutException("Timeout waiting for messages", ex);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> ConditionsStep<E> withSchema(Schema<E> schema) {
		this.schema = (Schema<T>) schema;
		return (ConditionsStep<E>) this;
	}

	private List<Message<T>> doGet() {
		var messages = new ArrayList<Message<T>>();
		await().atMost(this.timeout).until(() -> conditionHasBeenReached(messages));
		return messages;
	}

	private boolean conditionHasBeenReached(List<Message<T>> messages) throws PulsarClientException {
		try (Consumer<T> consumer = consumerFactory.createConsumer(this.schema, this.topics, "test-consumer",
				c -> c.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest))) {
			Message<T> message = consumer.receive();
			messages.add(message);
			consumer.acknowledge(message);
		}
		return this.condition.meets(messages);
	}

}

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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.util.Assert;

/**
 * Utility for consuming messages from Pulsar topics.
 * <p>
 * Exposes a Fluent builder-style API to construct the specifications for the message
 * consumption.
 *
 * @param <T> the type of the message payload
 * @author Jonas Geiregat
 */
public class PulsarConsumerTestUtil<T> implements TopicSpec<T>, SchemaSpec<T>, ConditionsSpec<T> {

	private final PulsarConsumerFactory<T> consumerFactory;

	private ConsumedMessagesCondition<T> condition;

	private Schema<T> schema;

	private Duration timeout = Duration.ofSeconds(30);

	private List<String> topics;

	public static <T> TopicSpec<T> consumeMessages() throws PulsarClientException {
		PulsarClient pulsarClient;
		try {
			pulsarClient = PulsarClient.builder().serviceUrl(
					"pulsar://localhost:6650"
			).build();
		}
		catch (PulsarClientException e) {
			pulsarClient = PulsarClient.builder().serviceUrl(
					PulsarTestContainerSupport.getPulsarBrokerUrl()
			).build();
		}
		PulsarConsumerFactory<T> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, List.of()
		);
		return new PulsarConsumerTestUtil<>(pulsarConsumerFactory);
	}

	public static <T> TopicSpec<T> consumeMessages(String url) throws PulsarClientException {
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(url).build();
		PulsarConsumerFactory<T> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, List.of()
		);
		return new PulsarConsumerTestUtil<>(pulsarConsumerFactory);
	}

	public static <T> TopicSpec<T> consumeMessages(PulsarClient pulsarClient) {
		PulsarConsumerFactory<T> pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(
				pulsarClient, List.of()
		);
		return new PulsarConsumerTestUtil<>(pulsarConsumerFactory);
	}

	public static <T> TopicSpec<T> consumeMessages(PulsarConsumerFactory<T> pulsarConsumerFactory) {
		return new PulsarConsumerTestUtil<>(pulsarConsumerFactory);
	}

	public PulsarConsumerTestUtil(PulsarConsumerFactory<T> consumerFactory) {
		Assert.notNull(consumerFactory, "PulsarConsumerFactory must not be null");
		this.consumerFactory = consumerFactory;
	}

	@Override
	public SchemaSpec<T> fromTopic(String topic) {
		Assert.notNull(topic, "Topic must not be null");
		this.topics = List.of(topic);
		return this;
	}

	@Override
	public ConditionsSpec<T> withSchema(Schema<T> schema) {
		Assert.notNull(schema, "Schema must not be null");
		this.schema = schema;
		return this;
	}

	@Override
	public ConditionsSpec<T> awaitAtMost(Duration timeout) {
		Assert.notNull(timeout, "Timeout must not be null");
		this.timeout = timeout;
		return this;
	}

	@Override
	public ConditionsSpec<T> until(ConsumedMessagesCondition<T> condition) {
		this.condition = condition;
		return this;
	}

	@Override
	public List<Message<T>> get() {
		var messages = new ArrayList<Message<T>>();
		try {
			var subscriptionName = "test-consumer-%s".formatted(UUID.randomUUID());
			try (Consumer<T> consumer = consumerFactory.createConsumer(this.schema, this.topics, subscriptionName,
					c -> c.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest))) {
				long remainingMillis = timeout.toMillis();
				do {
					long loopStartTime = System.currentTimeMillis();
					var message = consumer.receive(200, TimeUnit.MILLISECONDS);
					if (message != null) {
						messages.add(message);
						consumer.acknowledge(message);
					}
					if (this.condition != null && this.condition.meets(messages)) {
						return messages;
					}
					remainingMillis -= System.currentTimeMillis() - loopStartTime;
				}
				while (remainingMillis > 0);
			}
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
		if (this.condition != null && !this.condition.meets(messages)) {
			throw new ConditionTimeoutException(
					"Condition was not met within %d seconds".formatted(timeout.toSeconds()));
		}
		return messages;
	}

}

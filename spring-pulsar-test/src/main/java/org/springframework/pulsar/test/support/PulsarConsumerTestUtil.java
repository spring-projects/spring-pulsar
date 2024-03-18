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

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
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
 * @author Kartik Shrivastava
 * @author Chris Bono
 */
public final class PulsarConsumerTestUtil<T> implements TopicSpec<T>, SchemaSpec<T>, ConditionsSpec<T> {

	private static final LogAccessor LOG = new LogAccessor(PulsarConsumerTestUtil.class);

	private final PulsarClient locallyCreatedPulsarClient;

	private final PulsarConsumerFactory<T> consumerFactory;

	private ConsumedMessagesCondition<T> condition;

	private Schema<T> schema;

	private Duration timeout = Duration.ofSeconds(30);

	private List<String> topics;

	private boolean untilMethodAlreadyCalled = false;

	/**
	 * Begin a builder which will consume messages from the
	 * {@link PulsarTestContainerSupport} (if available) or the default Pulsar broker url
	 * {@code pulsar://localhost:6650}.
	 * @param <T> the message payload type
	 * @return the {@link TopicSpec topic step} of the builder
	 */
	public static <T> TopicSpec<T> consumeMessages() {
		if (PulsarTestContainerSupport.isContainerStarted()) {
			return PulsarConsumerTestUtil.consumeMessages(PulsarTestContainerSupport.getPulsarBrokerUrl());
		}
		return PulsarConsumerTestUtil.consumeMessages("pulsar://localhost:6650");
	}

	/**
	 * Begin a builder which will consume messages from the specified Pulsar broker url.
	 * @param <T> the message payload type
	 * @param url the Pulsar broker url
	 * @return the {@link TopicSpec topic step} of the builder
	 */
	public static <T> TopicSpec<T> consumeMessages(String url) {
		Assert.notNull(url, "url must not be null");
		try {
			var pulsarClient = PulsarClient.builder().serviceUrl(url).build();
			return PulsarConsumerTestUtil.consumeMessagesInternal(pulsarClient,
					new DefaultPulsarConsumerFactory<>(pulsarClient, List.of()));
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
	}

	/**
	 * Begin a builder which will consume messages with a provided Pulsar client.
	 * @param <T> the message payload type
	 * @param pulsarClient the client to consume with
	 * @return the {@link TopicSpec topic step} of the builder
	 */
	public static <T> TopicSpec<T> consumeMessages(PulsarClient pulsarClient) {
		Assert.notNull(pulsarClient, "pulsarClient must not be null");
		return PulsarConsumerTestUtil.consumeMessagesInternal(null,
				new DefaultPulsarConsumerFactory<>(pulsarClient, List.of()));
	}

	/**
	 * Begin a builder which will consume messages with a provided consumer factory.
	 * @param <T> the message payload type
	 * @param pulsarConsumerFactory the consumer factory to consume with
	 * @return the {@link TopicSpec topic step} of the builder
	 */
	public static <T> TopicSpec<T> consumeMessages(PulsarConsumerFactory<T> pulsarConsumerFactory) {
		return PulsarConsumerTestUtil.consumeMessagesInternal(null, pulsarConsumerFactory);
	}

	private static <T> TopicSpec<T> consumeMessagesInternal(PulsarClient locallyCreatedPulsarClient,
			PulsarConsumerFactory<T> pulsarConsumerFactory) {
		return new PulsarConsumerTestUtil<>(locallyCreatedPulsarClient, pulsarConsumerFactory);
	}

	private PulsarConsumerTestUtil(@Nullable PulsarClient locallyCreatedPulsarClient,
			PulsarConsumerFactory<T> consumerFactory) {
		Assert.notNull(consumerFactory, "PulsarConsumerFactory must not be null");
		this.consumerFactory = consumerFactory;
		this.locallyCreatedPulsarClient = locallyCreatedPulsarClient;
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
		if (untilMethodAlreadyCalled) {
			throw new IllegalStateException(
					"Multiple calls to 'until' are not allowed. Use 'and' to combine conditions.");
		}
		this.untilMethodAlreadyCalled = true;
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
		finally {
			if (this.locallyCreatedPulsarClient != null && !this.locallyCreatedPulsarClient.isClosed()) {
				try {
					this.locallyCreatedPulsarClient.close();
				}
				catch (PulsarClientException e) {
					LOG.error(e, () -> "Failed to close locally created Pulsar client due to: " + e.getMessage());
				}
			}
		}
		if (this.condition != null && !this.condition.meets(messages)) {
			throw new ConditionTimeoutException(
					"Condition was not met within %d seconds".formatted(timeout.toSeconds()));
		}
		return messages;
	}

}

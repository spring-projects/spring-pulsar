package org.springframework.pulsar.test.support;

import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Latest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;

public class PulsarTestConsumer {

	private final DefaultPulsarConsumerFactory<?> consumerFactory;

	private final Duration timeout;

	public PulsarTestConsumer(DefaultPulsarConsumerFactory<?> consumerFactory, Duration timeout) {
		this.consumerFactory = consumerFactory;
		this.timeout = timeout;
	}

	public PulsarTestConsumer(DefaultPulsarConsumerFactory<?> consumerFactory) {
		this(consumerFactory, Duration.ofSeconds(10));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> List<Message<T>> receiveEarliestMessages(int desiredMessageCount, List<String> topics, Schema<T> schema,
			SubscriptionInitialPosition initialPosition, String subscriptionName) {
		try (Consumer<T> consumer = consumerFactory.createConsumer((Schema) schema, topics, subscriptionName,
				c -> c.subscriptionInitialPosition(initialPosition))) {
			long remainingMillis = timeout.toMillis();
			List<Message<T>> received = new ArrayList<>();
			do {
				try {
					long loopStartTime = System.currentTimeMillis();
					Message<T> receive = consumer.receive(Math.toIntExact(remainingMillis), TimeUnit.MILLISECONDS);
					if (receive != null) {
						received.add(receive);
						consumer.acknowledge(receive);
					}
					remainingMillis -= System.currentTimeMillis() - loopStartTime;
				}
				catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}
			while (received.size() < desiredMessageCount && remainingMillis > 0);
			return received;
		}
		catch (PulsarClientException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> List<Message<T>> receiveEarliestMessages(int desiredMessageCount, List<String> topics,
			Schema<T> schema) {
		return receiveEarliestMessages(desiredMessageCount, topics, schema, Earliest, "test-subscription");
	}

	public <T> List<Message<T>> receiveLatestMessages(int desiredMessageCount, List<String> topics, Schema<T> schema) {
		return receiveEarliestMessages(desiredMessageCount, topics, schema, Latest, "test-subscription");
	}

}

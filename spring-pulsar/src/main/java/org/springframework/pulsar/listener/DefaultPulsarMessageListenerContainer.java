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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.event.ConsumerFailedToStartEvent;
import org.springframework.pulsar.event.ConsumerStartedEvent;
import org.springframework.pulsar.event.ConsumerStartingEvent;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation for {@link PulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Soby Chacko
 */
public class DefaultPulsarMessageListenerContainer<T> extends AbstractPulsarMessageListenerContainer<T> {

	private volatile boolean running = false;

	private volatile CompletableFuture<?> listenerConsumerFuture;

	private volatile Listener listenerConsumer;

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	private final AbstractPulsarMessageListenerContainer<?> thisOrParentContainer;

	public DefaultPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		super(pulsarConsumerFactory, pulsarContainerProperties);
		this.thisOrParentContainer = this;
	}

	@Override
	public void start() {
		doStart();
	}

	private void doStart() {

		PulsarContainerProperties containerProperties = getPulsarContainerProperties();

		Object messageListenerObject = containerProperties.getMessageListener();
		AsyncTaskExecutor consumerExecutor = containerProperties.getConsumerTaskExecutor();

		@SuppressWarnings("unchecked")
		MessageListener<T> messageListener = (MessageListener<T>) messageListenerObject;

		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}

		this.listenerConsumer = new Listener(messageListener);
		setRunning(true);
		this.startLatch = new CountDownLatch(1);
		this.listenerConsumerFuture = consumerExecutor.submitCompletable(this.listenerConsumer);

		try {
			if (!this.startLatch.await(containerProperties.getConsumerStartTimeout().toMillis(),
					TimeUnit.MILLISECONDS)) {
				this.logger.error("Consumer thread failed to start - does the configured task executor "
						+ "have enough threads to support all containers and concurrency?");
				publishConsumerFailedToStart();
			}
		}
		catch (@SuppressWarnings("UNUSED") InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void stop() {
		setRunning(false);
		this.logger.info("Pausing this consumer.");
		this.listenerConsumer.consumer.pause();
		try {
			this.logger.info("Closing this consumer.");
			this.listenerConsumer.consumer.close();
		}
		catch (PulsarClientException e) {
			this.logger.error(e, () -> "Error closing Pulsar Client.");
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public void destroy() {

	}

	private void publishConsumerStartingEvent() {
		this.startLatch.countDown();
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartingEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerStartedEvent() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartedEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerFailedToStart() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerFailedToStartEvent(this, this.thisOrParentContainer));
		}
	}

	private final class Listener implements SchedulingAwareRunnable {

		private final PulsarRecordMessageListener<T> listener;

		private final PulsarBatchMessageListener<T> batchMessageListener;

		private Consumer<T> consumer;

		private final Set<MessageId> nackableMessages = new HashSet<>();

		private final PulsarContainerProperties containerProperties = getPulsarContainerProperties();

		private volatile Thread consumerThread;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Listener(MessageListener<?> messageListener) {
			if (messageListener instanceof PulsarBatchMessageListener) {
				this.batchMessageListener = (PulsarBatchMessageListener<T>) messageListener;
				this.listener = null;
			}
			else if (messageListener != null) {
				this.listener = (PulsarRecordMessageListener<T>) messageListener;
				this.batchMessageListener = null;
			}
			else {
				this.listener = null;
				this.batchMessageListener = null;
			}
			try {
				final PulsarContainerProperties pulsarContainerProperties = getPulsarContainerProperties();
				Map<String, Object> propertiesToOverride = extractPropertiesToOverride(pulsarContainerProperties);

				final BatchReceivePolicy batchReceivePolicy = new BatchReceivePolicy.Builder()
						.maxNumMessages(pulsarContainerProperties.getMaxNumMessages())
						.maxNumBytes(pulsarContainerProperties.getMaxNumBytes())
						.timeout(pulsarContainerProperties.getBatchTimeout(), TimeUnit.MILLISECONDS).build();
				this.consumer = getPulsarConsumerFactory().createConsumer(
						(Schema) pulsarContainerProperties.getSchema(), batchReceivePolicy, propertiesToOverride);
			}
			catch (PulsarClientException e) {
				DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Pulsar client exceptions.");
			}
		}

		private Map<String, Object> extractPropertiesToOverride(PulsarContainerProperties pulsarContainerProperties) {

			Properties propertyOverrides = this.containerProperties.getPulsarConsumerProperties();

			final Map<String, Object> propOverridesAsMap = propertyOverrides.entrySet().stream().collect(Collectors
					.toMap(e -> String.valueOf(e.getKey()), Map.Entry::getValue, (prev, next) -> next, HashMap::new));

			final Map<String, Object> propertiesToOverride = new HashMap<>(propOverridesAsMap);
			if (propertiesToOverride.containsKey("topicNames")) {
				final String topicsFromMap = (String) propertiesToOverride.get("topicNames");
				final String[] topicNames = topicsFromMap.split(",");
				final Set<String> propertiesDefinedTopics = new HashSet<>(Arrays.stream(topicNames).toList());
				if (!propertiesDefinedTopics.isEmpty()) {
					propertiesToOverride.put("topicNames", propertiesDefinedTopics);
				}
			}

			if (!propertiesToOverride.containsKey("subscriptionType")) {
				final SubscriptionType subscriptionType = pulsarContainerProperties.getSubscriptionType();
				if (subscriptionType != null) {
					propertiesToOverride.put("subscriptionType", subscriptionType);
				}
			}
			if (!propertiesToOverride.containsKey("topicNames")) {
				final String[] topics = pulsarContainerProperties.getTopics();
				final Set<String> listenerDefinedTopics = new HashSet<>(Arrays.stream(topics).toList());
				if (!listenerDefinedTopics.isEmpty()) {
					propertiesToOverride.put("topicNames", listenerDefinedTopics);
				}
			}
			if (!propertiesToOverride.containsKey("subscriptionName")) {
				if (StringUtils.hasText(pulsarContainerProperties.getSubscriptionName())) {
					propertiesToOverride.put("subscriptionName", pulsarContainerProperties.getSubscriptionName());
				}
			}
			return propertiesToOverride;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			publishConsumerStartingEvent();
			this.consumerThread = Thread.currentThread();

			publishConsumerStartedEvent();
			while (isRunning()) {
				Messages<T> messages = null;

				// Always receive messages in batch mode.
				try {
					messages = this.consumer.batchReceive();
				}
				catch (PulsarClientException e) {
					DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Error receiving messages.");
				}
				Assert.isTrue(messages != null, "Messages cannot be null.");
				if (this.containerProperties.isBatchListener()) {
					try {
						if (messages.size() > 0) {
							if (this.batchMessageListener instanceof PulsarBatchAcknowledgingMessageListener) {
								this.batchMessageListener.received(this.consumer, messages,
										this.containerProperties
												.getAckMode() == PulsarContainerProperties.AckMode.MANUAL
														? new ConsumerBatchAcknowledgment(this.consumer) : null);
							}
							else {
								this.batchMessageListener.received(this.consumer, messages);
							}
							if (this.containerProperties.getAckMode() == PulsarContainerProperties.AckMode.BATCH) {
								try {
									if (isSharedSubsriptionType()) {
										this.consumer.acknowledge(messages);
									}
									else {
										final Stream<Message<T>> stream = StreamSupport.stream(messages.spliterator(),
												true);
										Message<T> last = stream.reduce((a, b) -> b).orElse(null);
										this.consumer.acknowledgeCumulative(last);
									}
								}
								catch (PulsarClientException pce) {
									this.consumer.negativeAcknowledge(messages);
								}
							}
						}
					}
					catch (Exception e) {
						// the whole batch is negatively acknowledged in the event of an
						// exception from the handler method.
						this.consumer.negativeAcknowledge(messages);
					}
				}
				else {
					for (Message<T> message : messages) {
						try {
							if (this.listener instanceof PulsarAcknowledgingMessageListener) {
								this.listener.received(this.consumer, message,
										this.containerProperties
												.getAckMode() == PulsarContainerProperties.AckMode.MANUAL
														? new ConsumerAcknowledgment(this.consumer, message) : null);
							}
							else if (this.listener != null) {
								this.listener.received(this.consumer, message);
							}
							if (this.containerProperties.getAckMode() == PulsarContainerProperties.AckMode.RECORD) {
								handleAck(message);
							}
						}
						catch (Exception e) {
							if (this.containerProperties.getAckMode() == PulsarContainerProperties.AckMode.RECORD) {
								this.consumer.negativeAcknowledge(message);
							}
							else if (this.containerProperties.getAckMode() == PulsarContainerProperties.AckMode.BATCH) {
								this.nackableMessages.add(message.getMessageId());
							}
						}
					}
					// All the records are processed at this point. Handle acks.
					if (this.containerProperties.getAckMode() == PulsarContainerProperties.AckMode.BATCH) {
						handleAcks(messages);
					}
				}
			}
		}

		private boolean isSharedSubsriptionType() {
			return this.containerProperties.getSubscriptionType() == SubscriptionType.Shared
					|| this.containerProperties.getSubscriptionType() == SubscriptionType.Key_Shared;
		}

		private void handleAcks(Messages<T> messages) {
			if (this.nackableMessages.isEmpty()) {
				try {
					if (messages.size() > 0) {
						if (isSharedSubsriptionType()) {
							this.consumer.acknowledge(messages);
						}
						else {
							final Stream<Message<T>> stream = StreamSupport.stream(messages.spliterator(), true);
							Message<T> last = stream.reduce((a, b) -> b).orElse(null);
							this.consumer.acknowledgeCumulative(last);
						}
					}
				}
				catch (PulsarClientException pce) {
					this.consumer.negativeAcknowledge(messages);
				}
			}
			else {
				for (Message<T> message : messages) {
					if (this.nackableMessages.contains(message.getMessageId())) {
						this.consumer.negativeAcknowledge(message);
						this.nackableMessages.remove(message.getMessageId());
					}
					else {
						handleAck(message);
					}
				}
			}
		}

		private void handleAck(Message<T> message) {
			try {
				this.consumer.acknowledge(message);
			}
			catch (PulsarClientException pce) {
				this.consumer.negativeAcknowledge(message);
			}
		}

	}

	private static final class ConsumerAcknowledgment implements Acknowledgement {

		private final Consumer<?> consumer;

		private final Message<?> message;

		ConsumerAcknowledgment(Consumer<?> consumer, Message<?> message) {
			this.consumer = consumer;
			this.message = message;
		}

		@Override
		public void acknowledge() {
			try {
				this.consumer.acknowledge(this.message);
			}
			catch (PulsarClientException e) {
				this.consumer.negativeAcknowledge(this.message);
			}
		}

		@Override
		public void acknowledge(MessageId messageId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void acknowledge(List<MessageId> messageIds) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void nack() {
			this.consumer.negativeAcknowledge(this.message);
		}

		@Override
		public void nack(MessageId messageId) {
			throw new UnsupportedOperationException();
		}

	}

	private static final class ConsumerBatchAcknowledgment implements Acknowledgement {

		private final Consumer<?> consumer;

		ConsumerBatchAcknowledgment(Consumer<?> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void acknowledge() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void acknowledge(MessageId messageId) {
			try {
				this.consumer.acknowledge(messageId);
			}
			catch (PulsarClientException e) {
				this.consumer.negativeAcknowledge(messageId);
			}
		}

		@Override
		public void acknowledge(List<MessageId> messageIds) {
			try {
				this.consumer.acknowledge(messageIds);
			}
			catch (PulsarClientException e) {
				for (MessageId messageId : messageIds) {
					try {
						this.consumer.acknowledge(messageId);
					}
					catch (PulsarClientException ex) {
						this.consumer.negativeAcknowledge(messageId);
					}
				}
			}
		}

		@Override
		public void nack() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void nack(MessageId messageId) {
			this.consumer.negativeAcknowledge(messageId);
		}

	}

}

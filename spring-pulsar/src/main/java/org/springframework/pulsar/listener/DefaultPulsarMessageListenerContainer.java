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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
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
 * @author Alexander Preuß
 * @author Chris Bono
 */
public class DefaultPulsarMessageListenerContainer<T> extends AbstractPulsarMessageListenerContainer<T> {

	private volatile Future<?> listenerConsumerFuture;

	private volatile Listener listenerConsumer;

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	private final AbstractPulsarMessageListenerContainer<?> thisOrParentContainer;

	public DefaultPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		super(pulsarConsumerFactory, pulsarContainerProperties);
		this.thisOrParentContainer = this;
	}

	@Override
	protected void doStart() {

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
		this.listenerConsumerFuture = consumerExecutor.submit(this.listenerConsumer);

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
	public void doStop() {
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

		private final PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

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
			this.pulsarConsumerErrorHandler = getPulsarConsumerErrorHandler();
			try {
				final PulsarContainerProperties pulsarContainerProperties = getPulsarContainerProperties();
				Map<String, Object> propertiesToConsumer = extractDirectConsumerProperties();
				populateAllNecessaryPropertiesIfNeedBe(propertiesToConsumer);

				final BatchReceivePolicy batchReceivePolicy = new BatchReceivePolicy.Builder()
						.maxNumMessages(pulsarContainerProperties.getMaxNumMessages())
						.maxNumBytes(pulsarContainerProperties.getMaxNumBytes())
						.timeout(pulsarContainerProperties.getBatchTimeout(), TimeUnit.MILLISECONDS).build();
				this.consumer = getPulsarConsumerFactory().createConsumer(
						(Schema) pulsarContainerProperties.getSchema(), batchReceivePolicy, propertiesToConsumer);
				Assert.state(this.consumer != null, "Unable to create a consumer");
			}
			catch (PulsarClientException e) {
				DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Pulsar client exceptions.");
			}
		}

		private Map<String, Object> extractDirectConsumerProperties() {
			Properties propertyOverrides = this.containerProperties.getPulsarConsumerProperties();
			return propertyOverrides.entrySet().stream().collect(Collectors.toMap(e -> String.valueOf(e.getKey()),
					Map.Entry::getValue, (prev, next) -> next, HashMap::new));
		}

		private void populateAllNecessaryPropertiesIfNeedBe(Map<String, Object> currentProperties) {
			if (currentProperties.containsKey("topicNames")) {
				final String topicsFromMap = (String) currentProperties.get("topicNames");
				final String[] topicNames = StringUtils.delimitedListToStringArray(topicsFromMap, ",");
				final Set<String> propertiesDefinedTopics = Set.of(topicNames);
				if (!propertiesDefinedTopics.isEmpty()) {
					currentProperties.put("topicNames", propertiesDefinedTopics);
				}
			}
			if (!currentProperties.containsKey("subscriptionType")) {
				final SubscriptionType subscriptionType = this.containerProperties.getSubscriptionType();
				if (subscriptionType != null) {
					currentProperties.put("subscriptionType", subscriptionType);
				}
			}
			if (!currentProperties.containsKey("topicNames")) {
				final String[] topics = this.containerProperties.getTopics();
				final Set<String> listenerDefinedTopics = new HashSet<>(Arrays.stream(topics).toList());
				if (!listenerDefinedTopics.isEmpty()) {
					currentProperties.put("topicNames", listenerDefinedTopics);
				}
			}
			if (!currentProperties.containsKey("topicsPattern")) {
				final String topicsPattern = this.containerProperties.getTopicsPattern();
				if (topicsPattern != null) {
					currentProperties.put("topicsPattern", topicsPattern);
				}
			}
			if (!currentProperties.containsKey("subscriptionName")) {
				if (StringUtils.hasText(this.containerProperties.getSubscriptionName())) {
					currentProperties.put("subscriptionName", this.containerProperties.getSubscriptionName());
				}
			}
			final RedeliveryBackoff negativeAckRedeliveryBackoff = DefaultPulsarMessageListenerContainer.this.negativeAckRedeliveryBackoff;
			if (negativeAckRedeliveryBackoff != null) {
				currentProperties.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
			}
			final DeadLetterPolicy deadLetterPolicy = DefaultPulsarMessageListenerContainer.this.deadLetterPolicy;
			if (deadLetterPolicy != null) {
				currentProperties.put("deadLetterPolicy", deadLetterPolicy);
			}
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
			AtomicBoolean inRetryMode = new AtomicBoolean(false);
			AtomicBoolean messagesPendingInBatch = new AtomicBoolean(false);
			Messages<T> messages = null;
			List<Message<T>> messageList = null;
			while (isRunning()) {
				// Always receive messages in batch mode.
				try {
					if (!inRetryMode.get() && !messagesPendingInBatch.get()) {
						messages = this.consumer.batchReceive();
					}
				}
				catch (PulsarClientException e) {
					DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Error receiving messages.");
				}
				Assert.isTrue(messages != null, "Messages cannot be null.");
				if (this.containerProperties.isBatchListener()) {
					if (!inRetryMode.get() && !messagesPendingInBatch.get()) {
						messageList = new ArrayList<>();
						messages.forEach(messageList::add);
					}
					try {
						if (messageList != null && messageList.size() > 0) {
							if (this.batchMessageListener instanceof PulsarBatchAcknowledgingMessageListener) {
								this.batchMessageListener.received(this.consumer, messageList,
										this.containerProperties.getAckMode() == AckMode.MANUAL
												? new ConsumerBatchAcknowledgment(this.consumer) : null);
							}
							else {
								this.batchMessageListener.received(this.consumer, messageList);
							}
							if (this.containerProperties.getAckMode() == AckMode.BATCH) {
								try {
									if (isSharedSubscriptionType()) {
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
							if (this.pulsarConsumerErrorHandler != null) {
								pendingMessagesHandledSuccessfully(inRetryMode, messagesPendingInBatch);
							}
						}
					}
					catch (Exception e) {
						if (this.pulsarConsumerErrorHandler != null) {
							messageList = invokeBatchListenerErrorHandler(inRetryMode, messagesPendingInBatch,
									messageList, e);
						}
						else {
							// the whole batch is negatively acknowledged in the event of
							// an exception from the handler method.
							this.consumer.negativeAcknowledge(messages);
						}
					}
				}
				else {
					for (Message<T> message : messages) {
						do {
							try {
								if (this.listener instanceof PulsarAcknowledgingMessageListener) {
									this.listener.received(this.consumer, message,
											this.containerProperties.getAckMode() == AckMode.MANUAL
													? new ConsumerAcknowledgment(this.consumer, message) : null);
								}
								else if (this.listener != null) {
									this.listener.received(this.consumer, message);
								}
								if (this.containerProperties.getAckMode() == AckMode.RECORD) {
									handleAck(message);
								}
								if (inRetryMode.get()) {
									inRetryMode.set(false);
								}
							}
							catch (Exception e) {
								if (this.pulsarConsumerErrorHandler != null) {
									invokeRecordListenerErrorHandler(inRetryMode, message, e);
								}
								else {
									if (this.containerProperties.getAckMode() == AckMode.RECORD) {
										this.consumer.negativeAcknowledge(message);
									}
									else if (this.containerProperties.getAckMode() == AckMode.BATCH) {
										this.nackableMessages.add(message.getMessageId());
									}
								}
							}
						}
						while (inRetryMode.get());
					}
					// All the records are processed at this point. Handle acks.
					if (this.containerProperties.getAckMode() == AckMode.BATCH) {
						handleAcks(messages);
					}
				}
			}
		}

		/**
		 * Special scenario for batch error handling round1: messages m1,m2,...m10 are
		 * received batch listener throws error on m3 goes through error handle flow and
		 * tracks m3 and sets messgeList to m3,m4..m10 round2: in retry mode, no new
		 * messages received If at this point all messages are handled successfully then
		 * the normal flow will clear the handler state out. However, if the handler
		 * throws an error again it will be one of 2 things... m3 or a subsequent message
		 * m4-m10.
		 * @param inRetryMode is the message in retry mode
		 * @param messagesPendingInBatch are there pe nding messages from the batch
		 * @param messageList message list to process
		 * @param exception exception from the failed message
		 * @return a list of messages to be processed next.
		 */
		private List<Message<T>> invokeBatchListenerErrorHandler(AtomicBoolean inRetryMode,
				AtomicBoolean messagesPendingInBatch, List<Message<T>> messageList, Exception exception) {
			Assert.isInstanceOf(PulsarBatchListenerFailedException.class, exception,
					"Batch listener should throw PulsarBatchListenerFailedException on errors.");
			PulsarBatchListenerFailedException pulsarBatchListenerFailedException = (PulsarBatchListenerFailedException) exception;
			Message<T> pulsarMessage = getPulsarMessageCausedTheException(pulsarBatchListenerFailedException);
			final Message<T> theCurrentPulsarMessageTracked = this.pulsarConsumerErrorHandler.currentMessage();
			// Previous message in error handled during retry but another msg in sublist
			// caused error;
			// resetting state in order to track it
			if (theCurrentPulsarMessageTracked != null && !theCurrentPulsarMessageTracked.equals(pulsarMessage)) {
				pendingMessagesHandledSuccessfully(inRetryMode, messagesPendingInBatch);
			}
			// this is key to understanding how the message gets retried, it gets put into
			// the new sublist
			// at position 0 (aka it will be the 1st one re-sent to the listener and see
			// if it can be
			// handled on the retry. Otherwise, if we are out of retries then the sublist
			// does not include
			// the message in error (it instead gets recovered).
			final int indexOfFailedMessage = messageList.indexOf(pulsarMessage);
			messageList = messageList.subList(indexOfFailedMessage, messageList.size());
			final boolean toBeRetried = this.pulsarConsumerErrorHandler
					.shouldRetryMessage(pulsarBatchListenerFailedException, pulsarMessage);
			if (toBeRetried) {
				inRetryMode.set(true);
			}
			else {
				if (inRetryMode.get()) {
					inRetryMode.set(false);
				}
				// retries exhausted - recover the message
				this.pulsarConsumerErrorHandler.recoverMessage(this.consumer, pulsarMessage,
						pulsarBatchListenerFailedException);
				handleAck(pulsarMessage);
				if (messageList.size() == 1) {
					messagesPendingInBatch.set(false);
				}
				else {
					messageList = messageList.subList(1, messageList.size());
				}
				if (!messageList.isEmpty()) {
					messagesPendingInBatch.set(true);
				}
				this.pulsarConsumerErrorHandler.clearMessage();
			}
			return messageList;
		}

		private void invokeRecordListenerErrorHandler(AtomicBoolean inRetryMode, Message<T> message, Exception e) {
			final boolean toBeRetried = this.pulsarConsumerErrorHandler.shouldRetryMessage(e, message);
			if (toBeRetried) {
				inRetryMode.set(true);
			}
			else {
				if (inRetryMode.get()) {
					inRetryMode.set(false);
				}
				// retries exhausted - recover the message
				this.pulsarConsumerErrorHandler.recoverMessage(this.consumer, message, e);
				// retries exhausted - if record ackmode, acknowledge, otherwise normal
				// batch ack at the end
				if (this.containerProperties.getAckMode() == AckMode.RECORD) {
					handleAck(message);
				}
			}
		}

		private void pendingMessagesHandledSuccessfully(AtomicBoolean inRetryMode,
				AtomicBoolean messagesPendingInBatch) {
			if (inRetryMode.get()) {
				inRetryMode.set(false);
			}
			if (messagesPendingInBatch.get()) {
				messagesPendingInBatch.set(false);
			}
			this.pulsarConsumerErrorHandler.clearMessage();
		}

		@SuppressWarnings("unchecked")
		private Message<T> getPulsarMessageCausedTheException(PulsarBatchListenerFailedException exception) {
			return (Message<T>) exception.getMessageInError();
		}

		private boolean isSharedSubscriptionType() {
			return this.containerProperties.getSubscriptionType() == SubscriptionType.Shared
					|| this.containerProperties.getSubscriptionType() == SubscriptionType.Key_Shared;
		}

		private void handleAcks(Messages<T> messages) {
			if (this.nackableMessages.isEmpty()) {
				try {
					if (messages.size() > 0) {
						if (isSharedSubscriptionType()) {
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

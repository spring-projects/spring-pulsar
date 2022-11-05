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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.logging.LogFactory;
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
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.ConsumerBuilderConfigurationUtil;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.event.ConsumerFailedToStartEvent;
import org.springframework.pulsar.event.ConsumerStartedEvent;
import org.springframework.pulsar.event.ConsumerStartingEvent;
import org.springframework.pulsar.observation.DefaultPulsarListenerObservationConvention;
import org.springframework.pulsar.observation.PulsarListenerObservation;
import org.springframework.pulsar.observation.PulsarMessageReceiverContext;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Default implementation for {@link PulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 * @author Christophe Bornet
 */
public class DefaultPulsarMessageListenerContainer<T> extends AbstractPulsarMessageListenerContainer<T> {

	private volatile CompletableFuture<?> listenerConsumerFuture;

	private volatile Listener listenerConsumer;

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	private final AbstractPulsarMessageListenerContainer<?> thisOrParentContainer;

	private final AtomicReference<Thread> listenerConsumerThread = new AtomicReference<>();

	private final AtomicBoolean receiveInProgress = new AtomicBoolean();

	public DefaultPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		this(pulsarConsumerFactory, pulsarContainerProperties, null);
	}

	public DefaultPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties, @Nullable ObservationRegistry observationRegistry) {
		super(pulsarConsumerFactory, pulsarContainerProperties, observationRegistry);
		this.thisOrParentContainer = this;
	}

	@Override
	protected void doStart() {

		PulsarContainerProperties containerProperties = getContainerProperties();

		Object messageListenerObject = containerProperties.getMessageListener();
		AsyncTaskExecutor consumerExecutor = containerProperties.getConsumerTaskExecutor();

		@SuppressWarnings("unchecked")
		MessageListener<T> messageListener = (MessageListener<T>) messageListenerObject;

		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}

		this.listenerConsumer = new Listener(messageListener, this.getContainerProperties(),
				this.getObservationRegistry());
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
	public void doStop() {
		setRunning(false);
		this.logger.info("Pausing this consumer.");
		this.listenerConsumer.consumer.pause();
		if (this.listenerConsumerThread.get() != null) {
			// if there is a receive operation already in progress, we want to interrupt
			// the listener thread.
			if (this.receiveInProgress.get()) {
				// All the records received so far in the current batch receive will be
				// re-delivered.
				this.listenerConsumerThread.get().interrupt();
			}
			// if there is something other than receive operations are in progress,
			// such as ack operations, wait for the listener thread to complete them.
			try {
				this.listenerConsumerThread.get().join();
			}
			catch (InterruptedException e) {
				this.logger.error(e, () -> "Interrupting the main thread");
				Thread.currentThread().interrupt();
			}
		}
		try {
			this.logger.info("Closing this consumer.");
			this.listenerConsumer.consumer.close();
		}
		catch (PulsarClientException e) {
			this.logger.error(e, () -> "Error closing Pulsar Client.");
		}
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

		private final PulsarContainerProperties containerProperties;

		private final ObservationRegistry observationRegistry;

		private Consumer<T> consumer;

		private final Set<MessageId> nackableMessages = new HashSet<>();

		private final PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

		private final boolean isBatchListener;

		private final AckMode ackMode;

		private final SubscriptionType subscriptionType;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Listener(MessageListener<?> messageListener, PulsarContainerProperties containerProperties,
				@Nullable ObservationRegistry observationRegistry) {

			this.containerProperties = containerProperties;
			this.isBatchListener = this.containerProperties.isBatchListener();
			this.ackMode = this.containerProperties.getAckMode();
			this.subscriptionType = this.containerProperties.getSubscriptionType();

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
			this.observationRegistry = observationRegistry;
			this.pulsarConsumerErrorHandler = getPulsarConsumerErrorHandler();
			try {
				Map<String, Object> propertiesToConsumer = extractDirectConsumerProperties();
				populateAllNecessaryPropertiesIfNeedBe(propertiesToConsumer);

				BatchReceivePolicy batchReceivePolicy = new BatchReceivePolicy.Builder()
						.maxNumMessages(containerProperties.getMaxNumMessages())
						.maxNumBytes(containerProperties.getMaxNumBytes())
						.timeout(containerProperties.getBatchTimeoutMillis(), TimeUnit.MILLISECONDS).build();

				/*
				 * topicNames and properties must not be added through the builder
				 * customizer as ConsumerBuilder::topics and ConsumerBuilder::properties
				 * don't replace but add to the existing topics/properties.
				 */
				Set<String> topicNames = (Set<String>) propertiesToConsumer.remove("topicNames");
				Map<String, String> properties = (Map<String, String>) propertiesToConsumer.remove("properties");

				ConsumerBuilderCustomizer<T> customizer = builder -> {
					ConsumerBuilderConfigurationUtil.loadConf(builder, propertiesToConsumer);
					builder.batchReceivePolicy(batchReceivePolicy);
				};
				this.consumer = getPulsarConsumerFactory().createConsumer((Schema) containerProperties.getSchema(),
						topicNames, properties, Collections.singletonList(customizer));
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
			final RedeliveryBackoff ackTimeoutRedeliveryBackoff = DefaultPulsarMessageListenerContainer.this.ackTimeoutRedeliveryBackoff;
			if (ackTimeoutRedeliveryBackoff != null) {
				currentProperties.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
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
			DefaultPulsarMessageListenerContainer.this.listenerConsumerThread.set(Thread.currentThread());
			publishConsumerStartingEvent();
			publishConsumerStartedEvent();
			AtomicBoolean inRetryMode = new AtomicBoolean(false);
			AtomicBoolean messagesPendingInBatch = new AtomicBoolean(false);
			Messages<T> messages = null;
			List<Message<T>> messageList = null;
			while (isRunning()) {
				// Always receive messages in batch mode.
				try {
					if (!inRetryMode.get() && !messagesPendingInBatch.get()) {
						DefaultPulsarMessageListenerContainer.this.receiveInProgress.set(true);
						messages = this.consumer.batchReceive();
					}
				}
				catch (PulsarClientException e) {
					if (e.getCause() instanceof InterruptedException) {
						DefaultPulsarMessageListenerContainer.this.logger.debug(e,
								() -> "Error receiving messages due to a thread interrupt call from upstream.");
					}
					else {
						DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Error receiving messages.");
					}
					messages = null;
				}
				finally {
					DefaultPulsarMessageListenerContainer.this.receiveInProgress.set(false);
				}

				if (messages == null) {
					continue;
				}

				if (this.isBatchListener) {
					if (!inRetryMode.get() && !messagesPendingInBatch.get()) {
						messageList = new ArrayList<>();
						messages.forEach(messageList::add);
					}
					try {
						if (messageList != null && messageList.size() > 0) {
							if (this.batchMessageListener instanceof PulsarBatchAcknowledgingMessageListener) {
								this.batchMessageListener.received(this.consumer, messageList,
										this.ackMode.equals(AckMode.MANUAL)
												? new ConsumerBatchAcknowledgment(this.consumer) : null);
							}
							else {
								this.batchMessageListener.received(this.consumer, messageList);
							}
							if (this.ackMode.equals(AckMode.BATCH)) {
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
							// the whole batch is negatively acknowledged in the event
							// of
							// an exception from the handler method.
							this.consumer.negativeAcknowledge(messages);
						}
					}
				}
				else {
					for (Message<T> message : messages) {
						do {
							newObservation(message).observe(() -> this.dispatchMessageToListener(message, inRetryMode));
						}
						while (inRetryMode.get());
					}
					// All the records are processed at this point. Handle acks.
					if (this.ackMode.equals(AckMode.BATCH)) {
						handleAcks(messages);
					}
				}
			}
		}

		private Observation newObservation(Message<T> message) {
			if (this.observationRegistry == null) {
				return Observation.NOOP;
			}
			return PulsarListenerObservation.LISTENER_OBSERVATION.observation(
					this.containerProperties.getObservationConvention(),
					DefaultPulsarListenerObservationConvention.INSTANCE,
					() -> new PulsarMessageReceiverContext(message, getBeanName()), this.observationRegistry);
		}

		private void dispatchMessageToListener(Message<T> message, AtomicBoolean inRetryMode) {
			try {
				if (this.listener instanceof PulsarAcknowledgingMessageListener) {
					this.listener.received(this.consumer, message, this.ackMode.equals(AckMode.MANUAL)
							? new ConsumerAcknowledgment(this.consumer, message) : null);
				}
				else if (this.listener != null) {
					this.listener.received(this.consumer, message);
				}
				if (this.ackMode.equals(AckMode.RECORD)) {
					handleAck(message);
				}
				inRetryMode.compareAndSet(true, false);
			}
			catch (Exception e) {
				if (this.pulsarConsumerErrorHandler != null) {
					invokeRecordListenerErrorHandler(inRetryMode, message, e);
				}
				else {
					if (this.ackMode.equals(AckMode.RECORD)) {
						this.consumer.negativeAcknowledge(message);
					}
					else if (this.ackMode.equals(AckMode.BATCH)) {
						this.nackableMessages.add(message.getMessageId());
					}
					else {
						throw new IllegalStateException(String.format(
								"Exception occurred and message %s was not auto-nacked; switch to AckMode BATCH or RECORD to enable auto-nacks",
								message.getMessageId()), e);
					}
				}
			}
		}

		/**
		 * Special scenario for batch error handling round1: messages m1,m2,...m10 are
		 * received batch listener throws error on m3 goes through error handle flow and
		 * tracks m3 and sets messageList to m3,m4..m10 round2: in retry mode, no new
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
				AtomicBoolean messagesPendingInBatch, List<Message<T>> messageList, Throwable exception) {

			// Make sure either the exception or the exception cause is batch exception
			if (!(exception instanceof PulsarBatchListenerFailedException)) {
				exception = exception.getCause();
				Assert.isInstanceOf(PulsarBatchListenerFailedException.class, exception,
						"Batch listener should throw PulsarBatchListenerFailedException on errors.");
			}

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
				inRetryMode.compareAndSet(true, false);
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
				inRetryMode.compareAndSet(true, false);
				// retries exhausted - recover the message
				this.pulsarConsumerErrorHandler.recoverMessage(this.consumer, message, e);
				// retries exhausted - if record ackmode, acknowledge, otherwise normal
				// batch ack at the end
				if (this.ackMode.equals(AckMode.RECORD)) {
					handleAck(message);
				}
			}
		}

		private void pendingMessagesHandledSuccessfully(AtomicBoolean inRetryMode,
				AtomicBoolean messagesPendingInBatch) {
			inRetryMode.compareAndSet(true, false);
			messagesPendingInBatch.compareAndSet(true, false);
			this.pulsarConsumerErrorHandler.clearMessage();
		}

		@SuppressWarnings("unchecked")
		private Message<T> getPulsarMessageCausedTheException(PulsarBatchListenerFailedException exception) {
			return (Message<T>) exception.getMessageInError();
		}

		private boolean isSharedSubscriptionType() {
			return this.subscriptionType.equals(SubscriptionType.Shared)
					|| this.subscriptionType.equals(SubscriptionType.Key_Shared);
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
			AbstractAcknowledgement.handleAckByMessageId(this.consumer, message.getMessageId());
		}

	}

	private static abstract class AbstractAcknowledgement implements Acknowledgement {

		private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(AbstractAcknowledgement.class));

		protected final Consumer<?> consumer;

		AbstractAcknowledgement(Consumer<?> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void acknowledge(MessageId messageId) {
			AbstractAcknowledgement.handleAckByMessageId(this.consumer, messageId);
		}

		private static void handleAckByMessageId(Consumer<?> consumer, MessageId messageId) {
			try {
				consumer.acknowledge(messageId);
			}
			catch (PulsarClientException pce) {
				AbstractAcknowledgement.logger.warn(pce,
						() -> String.format("Acknowledgment failed for message: [%s]", messageId));
				consumer.negativeAcknowledge(messageId);
			}
		}

		@Override
		public void acknowledge(List<MessageId> messageIds) {
			try {
				this.consumer.acknowledge(messageIds);
			}
			catch (PulsarClientException e) {
				for (MessageId messageId : messageIds) {
					handleAckByMessageId(this.consumer, messageId);
				}
			}
		}

		@Override
		public void nack(MessageId messageId) {
			this.consumer.negativeAcknowledge(messageId);
		}

	}

	private static final class ConsumerAcknowledgment extends AbstractAcknowledgement {

		private final Message<?> message;

		ConsumerAcknowledgment(Consumer<?> consumer, Message<?> message) {
			super(consumer);
			this.message = message;
		}

		@Override
		public void acknowledge() {
			acknowledge(this.message.getMessageId());
		}

		@Override
		public void nack() {
			this.consumer.negativeAcknowledge(this.message);
		}

	}

	private static final class ConsumerBatchAcknowledgment extends AbstractAcknowledgement {

		ConsumerBatchAcknowledgment(Consumer<?> consumer) {
			super(consumer);
		}

		@Override
		public void acknowledge() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void nack() {
			throw new UnsupportedOperationException();
		}

	}

}

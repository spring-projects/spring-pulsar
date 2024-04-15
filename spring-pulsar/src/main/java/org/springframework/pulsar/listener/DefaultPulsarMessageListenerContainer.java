/*
 * Copyright 2022-2024 the original author or authors.
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.ConsumerBuilderConfigurationUtil;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.event.ConsumerFailedToStartEvent;
import org.springframework.pulsar.event.ConsumerStartedEvent;
import org.springframework.pulsar.event.ConsumerStartingEvent;
import org.springframework.pulsar.listener.PulsarContainerProperties.TransactionSettings;
import org.springframework.pulsar.observation.DefaultPulsarListenerObservationConvention;
import org.springframework.pulsar.observation.PulsarListenerObservation;
import org.springframework.pulsar.observation.PulsarMessageReceiverContext;
import org.springframework.pulsar.transaction.PulsarAwareTransactionManager;
import org.springframework.pulsar.transaction.PulsarTransactionUtils;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import io.micrometer.observation.Observation;

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

	private final Lock lockOnPause = new ReentrantLock();

	private final Condition pausedCondition = this.lockOnPause.newCondition();

	public DefaultPulsarMessageListenerContainer(PulsarConsumerFactory<? super T> pulsarConsumerFactory,
			PulsarContainerProperties pulsarContainerProperties) {
		super(pulsarConsumerFactory, pulsarContainerProperties);
		this.thisOrParentContainer = this;
	}

	@Override
	protected void doStart() {
		PulsarContainerProperties containerProperties = getContainerProperties();
		AsyncTaskExecutor consumerExecutor = containerProperties.getConsumerTaskExecutor();
		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		@SuppressWarnings("unchecked")
		MessageListener<T> messageListener = (MessageListener<T>) containerProperties.getMessageListener();
		this.listenerConsumer = new Listener(messageListener, this.getContainerProperties());
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
		this.logger.info("Pausing consumer");
		if (this.listenerConsumer.consumer != null) {
			this.listenerConsumer.consumer.pause();
		}
		if (this.listenerConsumerThread.get() != null) {
			// If a receive op in progress then interrupt listener thread
			if (this.receiveInProgress.get()) {
				// All records already received in current batch will be re-delivered
				this.listenerConsumerThread.get().interrupt();
			}
			// If there is something other than receive operations are in progress,
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
			this.logger.info("Closing consumer");
			if (this.listenerConsumer.consumer != null) {
				this.listenerConsumer.consumer.close();
			}
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

	@Override
	public void doPause() {
		setPaused(true);
		if (this.listenerConsumer != null) {
			this.listenerConsumer.pause();
		}
	}

	@Override
	public void doResume() {
		if (this.listenerConsumer != null) {
			this.listenerConsumer.resume();
		}
		setPaused(false);
		this.lockOnPause.lock();
		try {
			// signal the lock's condition to continue.
			this.pausedCondition.signal();
		}
		finally {
			this.lockOnPause.unlock();
		}
	}

	private final class Listener implements SchedulingAwareRunnable {

		private final PulsarRecordMessageListener<T> listener;

		private final PulsarBatchMessageListener<T> batchMessageListener;

		private final PulsarContainerProperties containerProperties;

		private Consumer<T> consumer;

		private final Set<MessageId> nackableMessages = new HashSet<>();

		private final PulsarConsumerErrorHandler<T> pulsarConsumerErrorHandler;

		private final ConsumerBuilderCustomizer<T> consumerBuilderCustomizer;

		private final boolean isBatchListener;

		private final AckMode ackMode;

		private SubscriptionType subscriptionType;

		@Nullable
		private PulsarAwareTransactionManager transactionManager;

		@Nullable
		private TransactionTemplate transactionTemplate;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Listener(MessageListener<?> messageListener, PulsarContainerProperties containerProperties) {
			this.containerProperties = containerProperties;
			this.isBatchListener = this.containerProperties.isBatchListener();
			this.ackMode = this.containerProperties.getAckMode();
			this.subscriptionType = this.containerProperties.getSubscriptionType();
			validateTransactionSettings(this.containerProperties.transactions());
			this.transactionManager = this.containerProperties.transactions().getTransactionManager();
			this.transactionTemplate = determineTransactionTemplate();
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
			this.consumerBuilderCustomizer = getConsumerBuilderCustomizer();
			try {
				Map<String, Object> propertiesToConsumer = extractDirectConsumerProperties();
				populateAllNecessaryPropertiesIfNeedBe(propertiesToConsumer);

				BatchReceivePolicy batchReceivePolicy = new BatchReceivePolicy.Builder()
					.maxNumMessages(containerProperties.getMaxNumMessages())
					.maxNumBytes(containerProperties.getMaxNumBytes())
					.timeout(containerProperties.getBatchTimeoutMillis(), TimeUnit.MILLISECONDS)
					.build();

				/*
				 * topicNames and properties must not be added through the builder
				 * customizer as ConsumerBuilder::topics and ConsumerBuilder::properties
				 * don't replace but add to the existing topics/properties.
				 */
				Set<String> topicNames = (Set<String>) propertiesToConsumer.remove("topicNames");
				Map<String, String> properties = (Map<String, String>) propertiesToConsumer.remove("properties");

				List<ConsumerBuilderCustomizer<T>> customizers = new ArrayList<>();
				customizers.add(builder -> {
					ConsumerBuilderConfigurationUtil.loadConf(builder, propertiesToConsumer);
					builder.batchReceivePolicy(batchReceivePolicy);
				});
				if (this.consumerBuilderCustomizer != null) {
					customizers.add(this.consumerBuilderCustomizer);
				}

				this.consumer = getPulsarConsumerFactory().createConsumer((Schema) containerProperties.getSchema(),
						topicNames, this.containerProperties.getSubscriptionName(), properties, customizers);
				Assert.state(this.consumer != null, "Unable to create a consumer");

				// If subtype is null - update it based on the actual subtype of the
				// underlying consumer
				if (this.subscriptionType == null) {
					updateSubscriptionTypeFromConsumer(this.consumer);
				}
			}
			catch (PulsarException e) {
				DefaultPulsarMessageListenerContainer.this.logger.error(e, () -> "Pulsar exception.");
			}
		}

		private void validateTransactionSettings(TransactionSettings txnProps) {
			if (!txnProps.isEnabled()) {
				return;
			}
			var missingRequiredTxnMgr = txnProps.isRequired() && txnProps.getTransactionManager() == null;
			Assert.state(!missingRequiredTxnMgr, "Transactions are required but txn manager is null");

			var txnRecordListenerWithBatchAckMode = (txnProps.getTransactionManager() != null && !this.isBatchListener
					&& this.containerProperties.getAckMode() == AckMode.BATCH);
			Assert.state(!(txnRecordListenerWithBatchAckMode),
					"Transactional record listeners can not use batch ack mode");

			var batchListenerWithRecordAckMode = (this.isBatchListener
					&& this.containerProperties.getAckMode() == AckMode.RECORD);
			Assert.state(!(batchListenerWithRecordAckMode), "Batch record listeners do not support AckMode.RECORD");

			// TODO custom errorHandler w/ transactions not supported
		}

		@Nullable
		private TransactionTemplate determineTransactionTemplate() {
			if (this.transactionManager == null) {
				return null;
			}
			var template = new TransactionTemplate(this.transactionManager);
			var definition = this.containerProperties.transactions().determineTransactionDefinition();
			Assert.state(
					definition == null
							|| definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRED
							|| definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRES_NEW,
					"Transaction propagation behavior must be REQUIRED or REQUIRES_NEW");
			if (definition != null) {
				BeanUtils.copyProperties(definition, template);
			}
			return template;
		}

		private void updateSubscriptionTypeFromConsumer(Consumer<T> consumer) {
			try {
				var confField = ReflectionUtils.findField(ConsumerImpl.class, "conf");
				ReflectionUtils.makeAccessible(confField);
				var conf = ReflectionUtils.getField(confField, consumer);
				if (conf instanceof ConsumerConfigurationData<?> confData) {
					this.subscriptionType = confData.getSubscriptionType();
				}
			}
			catch (Exception ex) {
				DefaultPulsarMessageListenerContainer.this.logger.error(ex,
						() -> "Unable to determine default subscription type from consumer due to: " + ex.getMessage());
			}
		}

		private Map<String, Object> extractDirectConsumerProperties() {
			Properties propertyOverrides = this.containerProperties.getPulsarConsumerProperties();
			return propertyOverrides.entrySet()
				.stream()
				.collect(Collectors.toMap(e -> String.valueOf(e.getKey()), Map.Entry::getValue, (prev, next) -> next,
						HashMap::new));
		}

		private void populateAllNecessaryPropertiesIfNeedBe(Map<String, Object> currentProperties) {
			if (currentProperties.containsKey("topicNames")) {
				String topicsFromMap = (String) currentProperties.get("topicNames");
				String[] topicNames = StringUtils.delimitedListToStringArray(topicsFromMap, ",");
				Set<String> propertiesDefinedTopics = Set.of(topicNames);
				if (!propertiesDefinedTopics.isEmpty()) {
					currentProperties.put("topicNames", propertiesDefinedTopics);
				}
			}
			if (!currentProperties.containsKey("subscriptionType")) {
				SubscriptionType subscriptionType = this.containerProperties.getSubscriptionType();
				if (subscriptionType != null) {
					currentProperties.put("subscriptionType", subscriptionType);
				}
			}
			if (!currentProperties.containsKey("topicNames")) {
				Set<String> listenerDefinedTopics = this.containerProperties.getTopics();
				if (!this.containerProperties.getTopics().isEmpty()) {
					currentProperties.put("topicNames", listenerDefinedTopics);
				}
			}
			if (!currentProperties.containsKey("topicsPattern")) {
				String topicsPattern = this.containerProperties.getTopicsPattern();
				if (topicsPattern != null) {
					currentProperties.put("topicsPattern", topicsPattern);
				}
			}
			if (!currentProperties.containsKey("subscriptionName")) {
				if (StringUtils.hasText(this.containerProperties.getSubscriptionName())) {
					currentProperties.put("subscriptionName", this.containerProperties.getSubscriptionName());
				}
			}
			RedeliveryBackoff negativeAckRedeliveryBackoff = DefaultPulsarMessageListenerContainer.this.negativeAckRedeliveryBackoff;
			if (negativeAckRedeliveryBackoff != null) {
				currentProperties.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
			}
			RedeliveryBackoff ackTimeoutRedeliveryBackoff = DefaultPulsarMessageListenerContainer.this.ackTimeoutRedeliveryBackoff;
			if (ackTimeoutRedeliveryBackoff != null) {
				currentProperties.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
			}
			DeadLetterPolicy deadLetterPolicy = DefaultPulsarMessageListenerContainer.this.deadLetterPolicy;
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
				checkIfPausedAndHandleAccordingly();
				// Always receive messages in batch mode.
				try {
					if (!inRetryMode.get() && !messagesPendingInBatch.get()) {
						DefaultPulsarMessageListenerContainer.this.receiveInProgress.set(true);
						if (!isPaused()) {
							messages = this.consumer.batchReceive();
						}
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
					messageList = invokeBatchListener(messages, messageList, inRetryMode, messagesPendingInBatch);
				}
				else {
					invokeRecordListener(messages, inRetryMode);
				}
			}
		}

		public void pause() {
			if (this.consumer != null) {
				this.consumer.pause();
			}
		}

		public void resume() {
			if (this.consumer != null) {
				this.consumer.resume();
			}
		}

		private void checkIfPausedAndHandleAccordingly() {
			if (isPaused()) {
				// try acquiring the lock.
				DefaultPulsarMessageListenerContainer.this.lockOnPause.lock();
				try {
					// Waiting on lock's condition.
					DefaultPulsarMessageListenerContainer.this.pausedCondition.await();
				}
				catch (InterruptedException e) {
					throw new IllegalStateException("Exception occurred trying to wake up the paused listener thread.");
				}
				finally {
					DefaultPulsarMessageListenerContainer.this.lockOnPause.unlock();
				}
			}
		}

		private boolean transactional() {
			return this.transactionTemplate != null && this.transactionManager != null;
		}

		private void invokeRecordListener(Messages<T> messages, AtomicBoolean inRetryMode) {
			if (!this.transactional()) {
				doInvokeRecordListener(messages, inRetryMode);
			}
			else {
				invokeRecordListenerInTx(messages, inRetryMode);
			}
		}

		@Nullable
		private void doInvokeRecordListener(Messages<T> messages, AtomicBoolean inRetryMode) {
			for (Message<T> message : messages) {
				do {
					newObservation(message).observe(() -> this.dispatchMessageToListener(message, inRetryMode, null));
				}
				while (inRetryMode.get());
			}
			// All the records are processed at this point - handle acks
			if (this.ackMode.equals(AckMode.BATCH)) {
				handleBatchAcks(messages, null);
			}
		}

		private void invokeRecordListenerInTx(Messages<T> messages, AtomicBoolean inRetryMode) {
			for (Message<T> message : messages) {
				do {
					newObservation(message).observe(() -> this.dispatchMessageToListenerInTxn(message, inRetryMode));
				}
				while (inRetryMode.get());
			}
		}

		private Observation newObservation(Message<T> message) {
			if (this.containerProperties.getObservationRegistry() == null) {
				return Observation.NOOP;
			}
			return PulsarListenerObservation.LISTENER_OBSERVATION.observation(
					this.containerProperties.getObservationConvention(),
					DefaultPulsarListenerObservationConvention.INSTANCE,
					() -> new PulsarMessageReceiverContext(message, getBeanName()),
					this.containerProperties.getObservationRegistry());
		}

		private void dispatchMessageToListenerInTxn(Message<T> message, AtomicBoolean inRetryMode) {
			try {
				this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
					@Override
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						RuntimeException aborted = dispatchMessageToListener(message, inRetryMode, getTransaction());
						if (aborted != null) {
							throw aborted;
						}
					}
				});
			}
			catch (Throwable ex) {
				DefaultPulsarMessageListenerContainer.this.logger.error(ex, "Transaction rolled back");
			}
		}

		@Nullable
		private Transaction getTransaction() {
			if (this.transactionManager == null) {
				return null;
			}
			var resourceHolder = PulsarTransactionUtils.getResourceHolder(this.transactionManager.getPulsarClient());
			return resourceHolder.getTransaction();
		}

		private RuntimeException dispatchMessageToListener(Message<T> message, AtomicBoolean inRetryMode,
				@Nullable Transaction txn) {
			try {
				if (this.listener instanceof PulsarAcknowledgingMessageListener) {
					this.listener.received(this.consumer, message, this.ackMode.equals(AckMode.MANUAL)
							? new ConsumerAcknowledgment(this.consumer, message, txn) : null);
				}
				else if (this.listener != null) {
					this.listener.received(this.consumer, message);
				}
				if (this.ackMode.equals(AckMode.RECORD)) {
					handleAck(message, txn);
				}
				inRetryMode.compareAndSet(true, false);
			}
			catch (RuntimeException e) {
				DefaultPulsarMessageListenerContainer.this.logger.debug(e,
						() -> "Error dispatching the message to the listener.");
				if (this.pulsarConsumerErrorHandler != null) {
					invokeRecordListenerErrorHandler(inRetryMode, message, e, txn);
				}
				else {
					if (this.ackMode.equals(AckMode.RECORD)) {
						this.consumer.negativeAcknowledge(message);
					}
					else if (this.ackMode.equals(AckMode.BATCH)) {
						this.nackableMessages.add(message.getMessageId());
					}
					else {
						throw new IllegalStateException("Exception occurred and message %s was not auto-nacked; "
								+ "switch to AckMode BATCH or RECORD to enable auto-nacks"
									.formatted(message.getMessageId()),
								e);
					}
				}
				return e;
			}
			return null;
		}

		private void invokeRecordListenerErrorHandler(AtomicBoolean inRetryMode, Message<T> message, Exception e,
				@Nullable Transaction txn) {
			boolean toBeRetried = this.pulsarConsumerErrorHandler.shouldRetryMessage(e, message);
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
					handleAck(message, txn);
				}
			}
		}

		private List<Message<T>> invokeBatchListener(Messages<T> messages, List<Message<T>> messageList,
				AtomicBoolean inRetryMode, AtomicBoolean messagesPendingInBatch) {
			if (!this.transactional()) {
				return doInvokeBatchListener(messages, messageList, inRetryMode, messagesPendingInBatch, null);
			}
			return invokeBatchListenerInTxn(messages, messageList, inRetryMode, messagesPendingInBatch);
		}

		private List<Message<T>> invokeBatchListenerInTxn(Messages<T> messages, List<Message<T>> messageList,
				AtomicBoolean inRetryMode, AtomicBoolean messagesPendingInBatch) {
			try {
				return this.transactionTemplate.execute(status -> doInvokeBatchListener(messages, messageList,
						inRetryMode, messagesPendingInBatch, getTransaction()));
			}
			catch (Throwable e) {
				DefaultPulsarMessageListenerContainer.this.logger.error(e, "Transaction rolled back");
				return Collections.emptyList();
			}
		}

		private List<Message<T>> doInvokeBatchListener(Messages<T> messages, List<Message<T>> messageList,
				AtomicBoolean inRetryMode, AtomicBoolean messagesPendingInBatch, @Nullable Transaction txn) {
			try {
				if (!CollectionUtils.isEmpty(messageList)) {
					if (this.batchMessageListener instanceof PulsarBatchAcknowledgingMessageListener) {
						this.batchMessageListener.received(this.consumer, messageList,
								this.ackMode.equals(AckMode.MANUAL) ? new ConsumerBatchAcknowledgment(this.consumer)
										: null);
					}
					else {
						this.batchMessageListener.received(this.consumer, messageList);
					}
					if (this.ackMode.equals(AckMode.BATCH)) {
						try {
							if (isSharedSubscriptionType()) {
								AckUtils.handleAck(this.consumer, messages, txn);
							}
							else {
								Stream<Message<T>> stream = StreamSupport.stream(messages.spliterator(), true);
								Message<T> last = stream.reduce((a, b) -> b).orElse(null);
								AckUtils.handleAckCumulative(this.consumer, last, txn);
							}
						}
						catch (PulsarException pe) {
							DefaultPulsarMessageListenerContainer.this.logger.warn(pe,
									() -> "Batch acknowledgment failed: " + pe.getMessage());
							this.consumer.negativeAcknowledge(messages);
						}
					}
					if (this.pulsarConsumerErrorHandler != null) {
						pendingMessagesHandledSuccessfully(inRetryMode, messagesPendingInBatch);
					}
				}
				return Collections.emptyList();
			}
			catch (RuntimeException ex) {
				// TODO enforce no error handler w/ batch listener w/ txn
				if (this.pulsarConsumerErrorHandler != null) {
					return invokeBatchListenerErrorHandler(inRetryMode, messagesPendingInBatch, messageList, ex, txn);
				}
				// when no error handler nack the whole batch
				this.consumer.negativeAcknowledge(messages);
				if (txn != null) {
					throw ex;
				}
				return Collections.emptyList();
			}
		}

		// @formatter:off
		/**
		 * Special scenario for batch error handling.
		 * <p>Round1: messages m1,m2,...m10 are received batch listener throws error on
		 * m3 goes through error handle flow and tracks m3 and sets messageList to
		 * m3,m4..m10.
		 * <p>Round2: in retry mode, no new messages received. If at this point all
		 * messages are handled successfully then the normal flow will clear the handler
		 * state out. However, if the handler throws an error again it will be one of 2
		 * things... m3 or a subsequent message m4-m10.
		 * @param inRetryMode is the message in retry mode
		 * @param messagesPendingInBatch whether there pending messages from the batch
		 * @param messageList message list to process
		 * @param exception exception from the failed message
		 * @param txn transaction for the current thread - null when no transaction is in
		 * progress
		 * @return a list of messages to be processed next
		 */
		// @formatter:on
		private List<Message<T>> invokeBatchListenerErrorHandler(AtomicBoolean inRetryMode,
				AtomicBoolean messagesPendingInBatch, List<Message<T>> messageList, Throwable exception,
				@Nullable Transaction txn) {

			// Make sure either the exception or the exception cause is batch exception
			if (!(exception instanceof PulsarBatchListenerFailedException)) {
				exception = exception.getCause();
				Assert.isInstanceOf(PulsarBatchListenerFailedException.class, exception,
						"Batch listener should throw PulsarBatchListenerFailedException on errors.");
			}

			PulsarBatchListenerFailedException pulsarBatchListenerFailedException = (PulsarBatchListenerFailedException) exception;
			Message<T> pulsarMessage = getPulsarMessageCausedTheException(pulsarBatchListenerFailedException);
			Message<T> theCurrentPulsarMessageTracked = this.pulsarConsumerErrorHandler.currentMessage();
			// Previous message in error handled during retry but another msg in sublist
			// caused error; resetting state in order to track it
			if (theCurrentPulsarMessageTracked != null && !theCurrentPulsarMessageTracked.equals(pulsarMessage)) {
				pendingMessagesHandledSuccessfully(inRetryMode, messagesPendingInBatch);
			}
			// this is key to understanding how the message gets retried, it gets put into
			// the new sublist at position 0 (aka it will be the 1st one re-sent to the
			// listener and see if it can be handled on the retry. Otherwise, if we are
			// out of retries then the sublist does not include the message in error (it
			// instead gets recovered).
			int indexOfFailedMessage = messageList.indexOf(pulsarMessage);
			messageList = messageList.subList(indexOfFailedMessage, messageList.size());
			boolean toBeRetried = this.pulsarConsumerErrorHandler.shouldRetryMessage(pulsarBatchListenerFailedException,
					pulsarMessage);
			if (toBeRetried) {
				inRetryMode.set(true);
			}
			else {
				inRetryMode.compareAndSet(true, false);
				// retries exhausted - recover the message
				this.pulsarConsumerErrorHandler.recoverMessage(this.consumer, pulsarMessage,
						pulsarBatchListenerFailedException);
				handleAck(pulsarMessage, txn);
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
			return this.subscriptionType != null && (this.subscriptionType.equals(SubscriptionType.Shared)
					|| this.subscriptionType.equals(SubscriptionType.Key_Shared));
		}

		private void handleBatchAcks(Messages<T> messages, @Nullable Transaction txn) {
			if (this.nackableMessages.isEmpty()) {
				try {
					if (messages.size() > 0) {
						if (isSharedSubscriptionType()) {
							AckUtils.handleAck(this.consumer, messages, txn);
						}
						else {
							Stream<Message<T>> stream = StreamSupport.stream(messages.spliterator(), true);
							Message<T> last = stream.reduce((a, b) -> b).orElse(null);
							AckUtils.handleAckCumulative(this.consumer, last, txn);
						}
					}
				}
				catch (PulsarException pe) {
					DefaultPulsarMessageListenerContainer.this.logger.warn(pe,
							() -> "Batch acks failed: " + pe.getMessage());
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
						handleAck(message, txn);
					}
				}
			}
		}

		private void handleAck(Message<T> message, @Nullable Transaction txn) {
			AckUtils.handleAckWithNackOnFailure(this.consumer, message.getMessageId(), txn);
		}

	}

	private static abstract class AbstractAcknowledgement implements Acknowledgement {

		protected final Consumer<?> consumer;

		@Nullable
		private final Transaction txn;

		AbstractAcknowledgement(Consumer<?> consumer) {
			this(consumer, null);
		}

		AbstractAcknowledgement(Consumer<?> consumer, @Nullable Transaction txn) {
			this.consumer = consumer;
			this.txn = txn;
		}

		@Nullable
		protected Transaction getTransaction() {
			return this.txn;
		}

		@Override
		public void acknowledge(MessageId messageId) {
			handleAckByMessageId(messageId);
		}

		@Override
		public void acknowledge(List<MessageId> messageIds) {
			try {
				AckUtils.handleAck(this.consumer, messageIds, this.txn);
			}
			catch (PulsarException pe) {
				for (MessageId messageId : messageIds) {
					handleAckByMessageId(messageId);
				}
			}
		}

		@Override
		public void nack(MessageId messageId) {
			this.consumer.negativeAcknowledge(messageId);
		}

		protected void handleAckByMessageId(MessageId messageId) {
			AckUtils.handleAckWithNackOnFailure(this.consumer, messageId, this.txn);
		}

	}

	private static final class ConsumerAcknowledgment extends AbstractAcknowledgement {

		private final Message<?> message;

		ConsumerAcknowledgment(Consumer<?> consumer, Message<?> message, @Nullable Transaction txn) {
			super(consumer, txn);
			this.message = message;
		}

		@Override
		public void acknowledge() {
			handleAckByMessageId(this.message.getMessageId());
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

	static final class AckUtils {

		private static LogAccessor LOG = new LogAccessor(AckUtils.class);

		private AckUtils() {
		}

		static void handleAck(Consumer<?> consumer, MessageId messageId, @Nullable Transaction txn) {
			try {
				if (txn != null) {
					consumer.acknowledgeAsync(messageId, txn).get();
				}
				else {
					consumer.acknowledge(messageId);
				}
			}
			catch (Exception ex) {
				LOG.trace(ex, () -> "Ack for msg w/ id [%s] failed due to: %s".formatted(messageId, ex.getMessage()));
				throw PulsarException.unwrap(ex);
			}
		}

		static void handleAckWithNackOnFailure(Consumer<?> consumer, MessageId messageId, @Nullable Transaction txn) {
			try {
				AckUtils.handleAck(consumer, messageId, txn);
			}
			catch (Exception ex) {
				LOG.warn(ex, () -> "Ack for msg w/ id [%s] failed due to: %s".formatted(messageId, ex.getMessage()));
				consumer.negativeAcknowledge(messageId);
			}
		}

		static void handleAck(Consumer<?> consumer, List<MessageId> messageIds, @Nullable Transaction txn) {
			try {
				if (txn != null) {
					consumer.acknowledgeAsync(messageIds, txn).get();
				}
				else {
					consumer.acknowledge(messageIds);
				}
			}
			catch (Exception ex) {
				LOG.trace(ex, () -> "Batch ack failed due to: %s".formatted(ex.getMessage()));
				throw PulsarException.unwrap(ex);
			}
		}

		static void handleAck(Consumer<?> consumer, Messages<?> messages, @Nullable Transaction txn) {
			try {
				if (txn != null) {
					consumer.acknowledgeAsync(messages, txn).get();
				}
				else {
					consumer.acknowledge(messages);
				}
			}
			catch (Exception ex) {
				LOG.trace(ex, () -> "Batch ack failed due to: %s".formatted(ex.getMessage()));
				throw PulsarException.unwrap(ex);
			}
		}

		static void handleAckCumulative(Consumer<?> consumer, Message<?> last, @Nullable Transaction txn) {
			try {
				if (txn != null) {
					consumer.acknowledgeCumulativeAsync(last.getMessageId(), txn).get();
				}
				else {
					consumer.acknowledgeCumulative(last);
				}
			}
			catch (Exception ex) {
				LOG.trace(ex, () -> "Cumulative ack failed w/ last msg id [%s] due to: %s"
					.formatted(last.getMessageId(), ex.getMessage()));
				throw PulsarException.unwrap(ex);
			}
		}

	}

}

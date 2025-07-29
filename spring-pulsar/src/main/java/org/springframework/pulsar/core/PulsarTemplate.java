/*
 * Copyright 2022-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.api.transaction.Transaction;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.observation.DefaultPulsarTemplateObservationConvention;
import org.springframework.pulsar.observation.PulsarMessageSenderContext;
import org.springframework.pulsar.observation.PulsarTemplateObservation;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;
import org.springframework.pulsar.support.internal.logging.LambdaCustomizerWarnLogger;
import org.springframework.pulsar.transaction.PulsarTransactionUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * A template for executing high-level Pulsar operations.
 *
 * @param <T> the message payload type
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Jonas Geiregat
 */
public class PulsarTemplate<T>
		implements PulsarOperations<T>, ApplicationContextAware, BeanNameAware, SmartInitializingSingleton {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarProducerFactory<T> producerFactory;

	private final SchemaResolver schemaResolver;

	private final TopicResolver topicResolver;

	private final List<ProducerBuilderCustomizer<T>> interceptorsCustomizers;

	private final Map<Thread, Transaction> threadBoundTransactions = new HashMap<>();

	private final boolean isProducerFactoryCaching;

	/**
	 * Whether to record observations.
	 */
	private final boolean observationEnabled;

	/**
	 * The registry to record observations with.
	 */
	@Nullable
	private ObservationRegistry observationRegistry;

	/**
	 * The optional custom observation convention to use when recording observations.
	 */
	@Nullable
	private PulsarTemplateObservationConvention observationConvention;

	@Nullable
	private ApplicationContext applicationContext;

	private String beanName = "";

	/**
	 * Logs warning when Lambda is used for producer builder customizer.
	 */
	@Nullable
	private LambdaCustomizerWarnLogger lambdaLogger;

	/**
	 * Transaction settings.
	 */
	private final TransactionProperties transactionProps = new TransactionProperties();

	/**
	 * Construct a template instance without interceptors that uses the default schema
	 * resolver.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory) {
		this(producerFactory, Collections.emptyList());
	}

	/**
	 * Construct a template instance with interceptors that uses the default schema
	 * resolver and default topic resolver and enables observation recording.
	 * @param producerFactory the factory used to create the backing Pulsar producers.
	 * @param interceptors the interceptors to add to the producer.
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, List<ProducerInterceptor> interceptors) {
		this(producerFactory, interceptors, new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
	}

	/**
	 * Construct a template instance with optional observation configuration.
	 * @param producerFactory the factory used to create the backing Pulsar producers
	 * @param interceptors the list of interceptors to add to the producer
	 * @param schemaResolver the schema resolver to use
	 * @param topicResolver the topic resolver to use
	 * @param observationEnabled whether to record observations
	 */
	public PulsarTemplate(PulsarProducerFactory<T> producerFactory, List<ProducerInterceptor> interceptors,
			SchemaResolver schemaResolver, TopicResolver topicResolver, boolean observationEnabled) {
		this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory must not be null");
		this.schemaResolver = Objects.requireNonNull(schemaResolver, "schemaResolver must not be null");
		this.topicResolver = Objects.requireNonNull(topicResolver, "topicResolver must not be null");
		this.observationEnabled = observationEnabled;
		if (!CollectionUtils.isEmpty(interceptors)) {
			this.interceptorsCustomizers = interceptors.stream().map(this::adaptInterceptorToCustomizer).toList();
		}
		else {
			this.interceptorsCustomizers = null;
		}
		this.isProducerFactoryCaching = (this.producerFactory instanceof CachingPulsarProducerFactory<?>);
		this.lambdaLogger = newLambdaWarnLogger(1000);
	}

	private ProducerBuilderCustomizer<T> adaptInterceptorToCustomizer(ProducerInterceptor interceptor) {
		return b -> b.intercept(interceptor);
	}

	private LambdaCustomizerWarnLogger newLambdaWarnLogger(long frequency) {
		return new LambdaCustomizerWarnLogger(this.logger, frequency);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Gets the transaction properties.
	 * @return the transaction properties
	 * @since 1.1.0
	 */
	public TransactionProperties transactions() {
		return this.transactionProps;
	}

	/**
	 * How often to log a warning when a Lambda producer builder customizer is used.
	 * @param frequency how often to log warning (every Nth occurrence) or non-positive to
	 * not log warning.
	 */
	public void logWarningForLambdaCustomizer(long frequency) {
		this.lambdaLogger = (frequency > 0) ? newLambdaWarnLogger(frequency) : null;
	}

	/**
	 * If observations are enabled, attempt to obtain the Observation registry and
	 * convention.
	 */
	@Override
	public void afterSingletonsInstantiated() {
		if (!this.observationEnabled) {
			this.logger.debug(() -> "Observations are not enabled - not recording");
			return;
		}
		if (this.applicationContext == null) {
			this.logger.warn(() -> "Observations enabled but application context null - not recording");
			return;
		}
		this.observationRegistry = this.applicationContext.getBeanProvider(ObservationRegistry.class)
			.getIfUnique(() -> this.observationRegistry);
		this.observationConvention = this.applicationContext.getBeanProvider(PulsarTemplateObservationConvention.class)
			.getIfUnique(() -> this.observationConvention);
	}

	@Override
	public MessageId send(@Nullable T message) {
		return doSend(null, message, null, null, null, null);
	}

	@Override
	public MessageId send(@Nullable T message, @Nullable Schema<T> schema) {
		return doSend(null, message, schema, null, null, null);
	}

	@Override
	public MessageId send(@Nullable String topic, @Nullable T message) {
		return doSend(topic, message, null, null, null, null);
	}

	@Override
	public MessageId send(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema) {
		return doSend(topic, message, schema, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable T message) {
		return doSendAsync(null, message, null, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable T message, @Nullable Schema<T> schema) {
		return doSendAsync(null, message, schema, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable String topic, @Nullable T message) {
		return doSendAsync(topic, message, null, null, null, null);
	}

	@Override
	public CompletableFuture<MessageId> sendAsync(@Nullable String topic, @Nullable T message,
			@Nullable Schema<T> schema) {
		return doSendAsync(topic, message, schema, null, null, null);
	}

	@Override
	public SendMessageBuilder<T> newMessage(@Nullable T message) {
		return new SendMessageBuilderImpl<>(this, message);
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	private MessageId doSend(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema,
			@Nullable Collection<String> encryptionKeys,
			@Nullable TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer,
			@Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		try {
			return doSendAsync(topic, message, schema, encryptionKeys, typedMessageBuilderCustomizer,
					producerCustomizer)
				.get();
		}
		catch (PulsarException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new PulsarException(PulsarClientException.unwrap(ex));
		}
	}

	private CompletableFuture<MessageId> doSendAsync(@Nullable String topic, @Nullable T message,
			@Nullable Schema<T> schema, @Nullable Collection<String> encryptionKeys,
			@Nullable TypedMessageBuilderCustomizer<T> typedMessageBuilderCustomizer,
			@Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		String defaultTopic = Objects.toString(this.producerFactory.getDefaultTopic(), null);
		String topicName = this.topicResolver.resolveTopic(topic, message, () -> defaultTopic).orElseThrow();
		this.logger.trace(() -> "Sending msg to '%s' topic".formatted(topicName));

		PulsarMessageSenderContext senderContext = PulsarMessageSenderContext.newContext(topicName, this.beanName);
		Observation observation = newObservation(senderContext);
		Producer<T> producer = null;
		try {
			observation.start();
			producer = prepareProducerForSend(topicName, message, schema, encryptionKeys, producerCustomizer);
			var txn = getTransaction();
			var messageBuilder = (txn != null) ? producer.newMessage(txn) : producer.newMessage();
			messageBuilder = messageBuilder.value(message);
			if (typedMessageBuilderCustomizer != null) {
				typedMessageBuilderCustomizer.customize(messageBuilder);
			}
			// propagate props to message
			senderContext.properties().forEach(messageBuilder::property);
			var finalProducer = producer;
			return messageBuilder.sendAsync().whenComplete((msgId, ex) -> {
				if (ex == null) {
					this.logger.trace(() -> "Sent msg to '%s' topic".formatted(topicName));
					observation.stop();
				}
				else {
					this.logger.error(ex, () -> "Failed to send msg to '%s' topic".formatted(topicName));
					observation.error(ex);
					observation.stop();
				}
				ProducerUtils.closeProducerAsync(finalProducer, this.logger);
			});
		}
		catch (RuntimeException ex) {
			if (producer != null) {
				ProducerUtils.closeProducerAsync(producer, this.logger);
			}
			observation.error(ex);
			observation.stop();
			throw ex;
		}
	}

	private Observation newObservation(PulsarMessageSenderContext senderContext) {
		if (this.observationRegistry == null) {
			return Observation.NOOP;
		}
		return PulsarTemplateObservation.TEMPLATE_OBSERVATION.observation(this.observationConvention,
				DefaultPulsarTemplateObservationConvention.INSTANCE, () -> senderContext, this.observationRegistry);
	}

	@Nullable
	private Transaction getTransaction() {
		if (!this.transactions().isEnabled()) {
			return null;
		}
		boolean allowNonTransactional = !this.transactions().isRequired();
		boolean inTransaction = inTransaction();
		Assert.state(allowNonTransactional || inTransaction,
				"No transaction is in process; "
						+ "possible solutions: run the template operation within the scope of a "
						+ "template.executeInTransaction() operation, start a transaction with @Transactional "
						+ "before invoking the template method, "
						+ "run in a transaction started by a listener container when consuming a record");
		if (!inTransaction) {
			this.logger.trace(() -> "No txn found but allowNonTransactional is true - returning null");
			return null;
		}
		Transaction txn = this.threadBoundTransactions.get(Thread.currentThread());
		if (txn != null) {
			this.logger.trace(() -> "Found local template txn [%s]".formatted(txn));
			return txn;
		}
		// If we made it here there is already a Pulsar txn associated w/ the transaction
		// or there is an actual active transaction that we need to sync a Pulsar txn with
		// hence the call to 'obtainResourceHolder' rather than 'getResourceHolder'
		var resourceHolder = PulsarTransactionUtils.obtainResourceHolder(this.producerFactory.getPulsarClient(),
				this.transactions().getTimeout());
		return resourceHolder.getTransaction();
	}

	/**
	 * Determine if the template is currently running in either a local transaction or a
	 * transaction synchronized with the transaction resource manager.
	 * @return whether the template is currently running in a transaction
	 */
	private boolean inTransaction() {
		if (!this.transactions().isEnabled()) {
			return false;
		}
		return this.threadBoundTransactions.get(Thread.currentThread()) != null
				|| PulsarTransactionUtils.inTransaction(this.producerFactory.getPulsarClient());
	}

	private Producer<T> prepareProducerForSend(@Nullable String topic, @Nullable T message, @Nullable Schema<T> schema,
			@Nullable Collection<String> encryptionKeys, @Nullable ProducerBuilderCustomizer<T> producerCustomizer) {
		Schema<T> resolvedSchema = schema == null ? this.schemaResolver.resolveSchema(message).orElseThrow() : schema;
		List<ProducerBuilderCustomizer<T>> customizers = new ArrayList<>();
		if (!CollectionUtils.isEmpty(this.interceptorsCustomizers)) {
			customizers.addAll(this.interceptorsCustomizers);
		}
		if (producerCustomizer != null) {
			possiblyLogWarningOnUsingLambdaCustomizers(producerCustomizer);
			customizers.add(producerCustomizer);
		}
		return this.producerFactory.createProducer(resolvedSchema, topic, encryptionKeys, customizers);
	}

	private void possiblyLogWarningOnUsingLambdaCustomizers(ProducerBuilderCustomizer<T> producerCustomizer) {
		if (this.lambdaLogger != null && this.isProducerFactoryCaching) {
			this.lambdaLogger.maybeLog(producerCustomizer);
		}
	}

	/**
	 * Execute some arbitrary operation(s) on the template and return the result. The
	 * template is invoked within a local transaction and do not participate in a global
	 * transaction (if present).
	 * @param <R> the callback return type
	 * @param callback the callback
	 * @return the result
	 * @since 1.1.0
	 */
	@Nullable
	public <R> R executeInTransaction(TemplateCallback<T, R> callback) {
		Assert.notNull(callback, "callback must not be null");
		Assert.state(this.transactions().isEnabled(), "This template does not support transactions");
		var currentThread = Thread.currentThread();
		var txn = this.threadBoundTransactions.get(currentThread);
		Assert.state(txn == null, "Nested calls to 'executeInTransaction' are not allowed");
		txn = newPulsarTransaction();
		this.threadBoundTransactions.put(currentThread, txn);
		try {
			R result = callback.doWithTemplate(this);
			txn.commit().get();
			return result;
		}
		catch (Exception ex) {
			if (txn != null) {
				PulsarTransactionUtils.abort(txn);
			}
			throw PulsarException.unwrap(ex);
		}
		finally {
			this.threadBoundTransactions.remove(currentThread);
		}
	}

	private Transaction newPulsarTransaction() {
		try {
			var txnBuilder = this.producerFactory.getPulsarClient().newTransaction();
			if (this.transactions().getTimeout() != null) {
				long timeoutSecs = this.transactions().getTimeout().toSeconds();
				txnBuilder.withTransactionTimeout(timeoutSecs, TimeUnit.SECONDS);
			}
			return txnBuilder.build().get();
		}
		catch (Exception ex) {
			throw PulsarException.unwrap(ex);
		}
	}

	public static class SendMessageBuilderImpl<T> implements SendMessageBuilder<T> {

		private final PulsarTemplate<T> template;

		@Nullable
		private final T message;

		@Nullable
		private String topic;

		@Nullable
		private Schema<T> schema;

		@Nullable
		private Collection<String> encryptionKeys;

		@Nullable
		private TypedMessageBuilderCustomizer<T> messageCustomizer;

		@Nullable
		private ProducerBuilderCustomizer<T> producerCustomizer;

		SendMessageBuilderImpl(PulsarTemplate<T> template, @Nullable T message) {
			this.template = template;
			this.message = message;
		}

		@Override
		public SendMessageBuilder<T> withTopic(String topic) {
			this.topic = topic;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withSchema(Schema<T> schema) {
			this.schema = schema;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withEncryptionKeys(Collection<String> encryptionKeys) {
			this.encryptionKeys = encryptionKeys;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withMessageCustomizer(TypedMessageBuilderCustomizer<T> messageCustomizer) {
			this.messageCustomizer = messageCustomizer;
			return this;
		}

		@Override
		public SendMessageBuilder<T> withProducerCustomizer(ProducerBuilderCustomizer<T> producerCustomizer) {
			this.producerCustomizer = producerCustomizer;
			return this;
		}

		@Override
		public MessageId send() {
			return this.template.doSend(this.topic, this.message, this.schema, this.encryptionKeys,
					this.messageCustomizer, this.producerCustomizer);
		}

		@Override
		public CompletableFuture<MessageId> sendAsync() {
			return this.template.doSendAsync(this.topic, this.message, this.schema, this.encryptionKeys,
					this.messageCustomizer, this.producerCustomizer);
		}

	}

	/**
	 * A callback for executing arbitrary operations on a {@code PulsarTemplate}.
	 *
	 * @param <T> the template message payload type
	 * @param <R> the return type
	 * @since 1.1.0
	 */
	public interface TemplateCallback<T, R> {

		/**
		 * Callback method given a template to execute operations on.
		 * @param template the template
		 * @return the result of the operations or null if no result needed
		 */
		@Nullable
		R doWithTemplate(PulsarTemplate<T> template);

	}

}

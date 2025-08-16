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

package org.springframework.pulsar.listener;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.Nullable;

import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.core.TransactionProperties;
import org.springframework.pulsar.observation.PulsarListenerObservationConvention;
import org.springframework.pulsar.transaction.PulsarAwareTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;
import org.springframework.util.backoff.FixedBackOff;

import io.micrometer.observation.ObservationRegistry;

/**
 * Contains runtime properties for a listener container.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 * @author Vedran Pavic
 */
public class PulsarContainerProperties {

	private static final Duration DEFAULT_CONSUMER_START_TIMEOUT = Duration.ofSeconds(30);

	private static final String SUBSCRIPTION_NAME = "subscriptionName";

	private static final String SUBSCRIPTION_TYPE = "subscriptionType";

	private @Nullable Duration consumerStartTimeout = DEFAULT_CONSUMER_START_TIMEOUT;

	private @Nullable Set<String> topics;

	private @Nullable String topicsPattern;

	private @Nullable String subscriptionName;

	private @Nullable SubscriptionType subscriptionType;

	private @Nullable Schema<?> schema;

	private @Nullable SchemaType schemaType;

	private @Nullable SchemaResolver schemaResolver;

	private @Nullable TopicResolver topicResolver;

	private @Nullable Object messageListener;

	private @Nullable AsyncTaskExecutor consumerTaskExecutor;

	private int concurrency = 1;

	private int maxNumMessages = -1;

	private int maxNumBytes = 10 * 1024 * 1024;

	private int batchTimeoutMillis = 100;

	private boolean batchListener;

	private @Nullable AckMode ackMode = AckMode.BATCH;

	private boolean observationEnabled;

	private @Nullable ObservationRegistry observationRegistry;

	private @Nullable PulsarListenerObservationConvention observationConvention;

	private Properties pulsarConsumerProperties = new Properties();

	private final TransactionSettings transactions = new TransactionSettings();

	private @Nullable RetryTemplate startupFailureRetryTemplate;

	private final RetryTemplate defaultStartupFailureRetryTemplate = new RetryTemplate(
			RetryPolicy.builder().backOff(new FixedBackOff(Duration.ofSeconds(10).toMillis(), 3)).build());

	private StartupFailurePolicy startupFailurePolicy = StartupFailurePolicy.STOP;

	public PulsarContainerProperties(String... topics) {
		this.topics = Set.of(topics);
		this.topicsPattern = null;
		this.schemaResolver = new DefaultSchemaResolver();
		this.topicResolver = new DefaultTopicResolver();
	}

	public PulsarContainerProperties(String topicPattern) {
		this.topicsPattern = topicPattern;
		this.topics = null;
		this.schemaResolver = new DefaultSchemaResolver();
		this.topicResolver = new DefaultTopicResolver();
	}

	public @Nullable Object getMessageListener() {
		return this.messageListener;
	}

	public void setMessageListener(@Nullable Object messageListener) {
		this.messageListener = messageListener;
	}

	public @Nullable AsyncTaskExecutor getConsumerTaskExecutor() {
		return this.consumerTaskExecutor;
	}

	public void setConsumerTaskExecutor(@Nullable AsyncTaskExecutor consumerExecutor) {
		this.consumerTaskExecutor = consumerExecutor;
	}

	public int getConcurrency() {
		return this.concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public @Nullable SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(@Nullable SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public int getMaxNumMessages() {
		return this.maxNumMessages;
	}

	public void setMaxNumMessages(int maxNumMessages) {
		this.maxNumMessages = maxNumMessages;
	}

	public int getMaxNumBytes() {
		return this.maxNumBytes;
	}

	public void setMaxNumBytes(int maxNumBytes) {
		this.maxNumBytes = maxNumBytes;
	}

	public int getBatchTimeoutMillis() {
		return this.batchTimeoutMillis;
	}

	public void setBatchTimeoutMillis(int batchTimeoutMillis) {
		this.batchTimeoutMillis = batchTimeoutMillis;
	}

	public boolean isBatchListener() {
		return this.batchListener;
	}

	public void setBatchListener(boolean batchListener) {
		this.batchListener = batchListener;
	}

	public @Nullable AckMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(@Nullable AckMode ackMode) {
		this.ackMode = ackMode;
	}

	public boolean isObservationEnabled() {
		return this.observationEnabled;
	}

	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}

	public @Nullable ObservationRegistry getObservationRegistry() {
		return this.observationRegistry;
	}

	void setObservationRegistry(@Nullable ObservationRegistry observationRegistry) {
		this.observationRegistry = observationRegistry;
	}

	public @Nullable PulsarListenerObservationConvention getObservationConvention() {
		return this.observationConvention;
	}

	/**
	 * Set a custom observation convention.
	 * @param observationConvention the convention.
	 */
	void setObservationConvention(@Nullable PulsarListenerObservationConvention observationConvention) {
		this.observationConvention = observationConvention;
	}

	public @Nullable Duration getConsumerStartTimeout() {
		return this.consumerStartTimeout;
	}

	public Duration determineConsumerStartTimeout() {
		return this.consumerStartTimeout != null ? this.consumerStartTimeout : DEFAULT_CONSUMER_START_TIMEOUT;
	}

	/**
	 * Set the max duration to wait for the consumer thread to start before logging an
	 * error. The default is 30 seconds.
	 * @param consumerStartTimeout the consumer start timeout
	 */
	public void setConsumerStartTimeout(@Nullable Duration consumerStartTimeout) {
		this.consumerStartTimeout = consumerStartTimeout;
	}

	public @Nullable Set<String> getTopics() {
		return this.topics;
	}

	public void setTopics(@Nullable Set<String> topics) {
		this.topics = topics;
	}

	public @Nullable String getTopicsPattern() {
		return this.topicsPattern;
	}

	public void setTopicsPattern(@Nullable String topicsPattern) {
		this.topicsPattern = topicsPattern;
	}

	public @Nullable String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setSubscriptionName(@Nullable String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public @Nullable Schema<?> getSchema() {
		return this.schema;
	}

	public void setSchema(@Nullable Schema<?> schema) {
		this.schema = schema;
	}

	public @Nullable SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(@Nullable SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	public @Nullable SchemaResolver getSchemaResolver() {
		return this.schemaResolver;
	}

	public void setSchemaResolver(@Nullable SchemaResolver schemaResolver) {
		this.schemaResolver = schemaResolver;
	}

	public @Nullable TopicResolver getTopicResolver() {
		return this.topicResolver;
	}

	public void setTopicResolver(@Nullable TopicResolver topicResolver) {
		this.topicResolver = topicResolver;
	}

	public Properties getPulsarConsumerProperties() {
		return this.pulsarConsumerProperties;
	}

	public void setPulsarConsumerProperties(Properties pulsarConsumerProperties) {
		Assert.notNull(pulsarConsumerProperties, "pulsarConsumerProperties must not be null");
		this.pulsarConsumerProperties = pulsarConsumerProperties;
	}

	/**
	 * Gets the transaction settings for the listener container.
	 * @return the transaction settings
	 * @since 1.1.0
	 */
	public TransactionSettings transactions() {
		return this.transactions;
	}

	public @Nullable RetryTemplate getStartupFailureRetryTemplate() {
		return this.startupFailureRetryTemplate;
	}

	/**
	 * Get the default template to use to retry startup when no custom retry template has
	 * been specified.
	 * @return the default retry template that will retry 3 times with a fixed delay of 10
	 * seconds between each attempt.
	 * @since 1.2.0
	 */
	public RetryTemplate getDefaultStartupFailureRetryTemplate() {
		return this.defaultStartupFailureRetryTemplate;
	}

	/**
	 * Set the template to use to retry startup when an exception occurs during startup.
	 * @param startupFailureRetryTemplate the retry template to use
	 * @since 1.2.0
	 */
	public void setStartupFailureRetryTemplate(@Nullable RetryTemplate startupFailureRetryTemplate) {
		this.startupFailureRetryTemplate = startupFailureRetryTemplate;
		if (this.startupFailureRetryTemplate != null) {
			setStartupFailurePolicy(StartupFailurePolicy.RETRY);
		}
	}

	public @Nullable StartupFailurePolicy getStartupFailurePolicy() {
		return this.startupFailurePolicy;
	}

	/**
	 * The action to take on the container when a failure occurs during startup.
	 * @param startupFailurePolicy action to take when a failure occurs during startup
	 * @since 1.2.0
	 */
	public void setStartupFailurePolicy(@Nullable StartupFailurePolicy startupFailurePolicy) {
		this.startupFailurePolicy = Objects.requireNonNull(startupFailurePolicy,
				"startupFailurePolicy must not be null");
	}

	public void updateContainerProperties() {
		applyPropIfSpecified(SUBSCRIPTION_NAME, this::setSubscriptionName);
		applyPropIfSpecified(SUBSCRIPTION_TYPE, this::setSubscriptionType);
	}

	@SuppressWarnings("unchecked")
	private <T> void applyPropIfSpecified(String key, Consumer<T> setter) {
		if (this.pulsarConsumerProperties.containsKey(key)) {
			T value = (T) this.pulsarConsumerProperties.get(key);
			setter.accept(value);
		}
	}

	/**
	 * Transaction related settings.
	 *
	 * @since 1.1.0
	 */
	public static class TransactionSettings extends TransactionProperties {

		private @Nullable TransactionDefinition transactionDefinition;

		private @Nullable PulsarAwareTransactionManager transactionManager;

		/**
		 * Get the transaction definition.
		 * @return the definition
		 */
		public @Nullable TransactionDefinition getTransactionDefinition() {
			return this.transactionDefinition;
		}

		/**
		 * Set a transaction definition with properties (e.g. timeout) that will be copied
		 * to the container's transaction template.
		 * @param transactionDefinition the definition
		 */
		public void setTransactionDefinition(@Nullable TransactionDefinition transactionDefinition) {
			this.transactionDefinition = transactionDefinition;
		}

		/**
		 * Determines the transaction definition to use by respecting any user configured
		 * timeout property.
		 * @return the transaction definition to use including any user specified timeout
		 * setting
		 */
		public @Nullable TransactionDefinition determineTransactionDefinition() {
			var timeout = this.getTimeout();
			if (timeout == null) {
				return this.transactionDefinition;
			}
			var txnDef = (this.transactionDefinition != null)
					? new DefaultTransactionDefinition(this.transactionDefinition) : new DefaultTransactionDefinition();
			txnDef.setTimeout(Math.toIntExact(timeout.toSeconds()));
			return txnDef;
		}

		/**
		 * Gets the transaction manager used to start transactions.
		 * @return the transaction manager
		 */
		@Nullable public PulsarAwareTransactionManager getTransactionManager() {
			return this.transactionManager;
		}

		/**
		 * Set the transaction manager to start a transaction.
		 * @param transactionManager the transaction manager
		 */
		public void setTransactionManager(@Nullable PulsarAwareTransactionManager transactionManager) {
			this.transactionManager = transactionManager;
		}

	}

}

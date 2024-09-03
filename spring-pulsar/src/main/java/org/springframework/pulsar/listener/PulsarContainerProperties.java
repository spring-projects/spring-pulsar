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

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.lang.Nullable;
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

	private Duration consumerStartTimeout = DEFAULT_CONSUMER_START_TIMEOUT;

	private Set<String> topics;

	private String topicsPattern;

	private String subscriptionName;

	private SubscriptionType subscriptionType;

	private Schema<?> schema;

	private SchemaType schemaType;

	private SchemaResolver schemaResolver;

	private TopicResolver topicResolver;

	private Object messageListener;

	private AsyncTaskExecutor consumerTaskExecutor;

	private int concurrency = 1;

	private int maxNumMessages = -1;

	private int maxNumBytes = 10 * 1024 * 1024;

	private int batchTimeoutMillis = 100;

	private boolean batchListener;

	private AckMode ackMode = AckMode.BATCH;

	private boolean observationEnabled;

	private ObservationRegistry observationRegistry;

	private PulsarListenerObservationConvention observationConvention;

	private Properties pulsarConsumerProperties = new Properties();

	private final TransactionSettings transactions = new TransactionSettings();

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

	public Object getMessageListener() {
		return this.messageListener;
	}

	public void setMessageListener(Object messageListener) {
		this.messageListener = messageListener;
	}

	public AsyncTaskExecutor getConsumerTaskExecutor() {
		return this.consumerTaskExecutor;
	}

	public void setConsumerTaskExecutor(AsyncTaskExecutor consumerExecutor) {
		this.consumerTaskExecutor = consumerExecutor;
	}

	public int getConcurrency() {
		return this.concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(SubscriptionType subscriptionType) {
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

	public AckMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	public boolean isObservationEnabled() {
		return this.observationEnabled;
	}

	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}

	public ObservationRegistry getObservationRegistry() {
		return this.observationRegistry;
	}

	void setObservationRegistry(ObservationRegistry observationRegistry) {
		this.observationRegistry = observationRegistry;
	}

	public PulsarListenerObservationConvention getObservationConvention() {
		return this.observationConvention;
	}

	/**
	 * Set a custom observation convention.
	 * @param observationConvention the convention.
	 */
	void setObservationConvention(PulsarListenerObservationConvention observationConvention) {
		this.observationConvention = observationConvention;
	}

	public Duration getConsumerStartTimeout() {
		return this.consumerStartTimeout;
	}

	/**
	 * Set the timeout to wait for a consumer thread to start before logging an error.
	 * Default 30 seconds.
	 * @param consumerStartTimeout the consumer start timeout.
	 */
	public void setConsumerStartTimeout(Duration consumerStartTimeout) {
		Assert.notNull(consumerStartTimeout, "'consumerStartTimeout' cannot be null");
		this.consumerStartTimeout = consumerStartTimeout;
	}

	public Set<String> getTopics() {
		return this.topics;
	}

	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

	public String getTopicsPattern() {
		return this.topicsPattern;
	}

	public void setTopicsPattern(String topicsPattern) {
		this.topicsPattern = topicsPattern;
	}

	public String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public Schema<?> getSchema() {
		return this.schema;
	}

	public void setSchema(Schema<?> schema) {
		this.schema = schema;
	}

	public SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	public SchemaResolver getSchemaResolver() {
		return this.schemaResolver;
	}

	public void setSchemaResolver(SchemaResolver schemaResolver) {
		this.schemaResolver = schemaResolver;
	}

	public TopicResolver getTopicResolver() {
		return this.topicResolver;
	}

	public void setTopicResolver(TopicResolver topicResolver) {
		this.topicResolver = topicResolver;
	}

	public Properties getPulsarConsumerProperties() {
		return this.pulsarConsumerProperties;
	}

	public void setPulsarConsumerProperties(Properties pulsarConsumerProperties) {
		this.pulsarConsumerProperties = pulsarConsumerProperties;
	}

	/**
	 * Gets the transaction settings.
	 * @return the transaction settings
	 * @since 1.1.0
	 */
	public TransactionSettings transactions() {
		return this.transactions;
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

		@Nullable
		private TransactionDefinition transactionDefinition;

		@Nullable
		private PulsarAwareTransactionManager transactionManager;

		/**
		 * Get the transaction definition.
		 * @return the definition
		 */
		@Nullable
		public TransactionDefinition getTransactionDefinition() {
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
		public TransactionDefinition determineTransactionDefinition() {
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
		@Nullable
		public PulsarAwareTransactionManager getTransactionManager() {
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

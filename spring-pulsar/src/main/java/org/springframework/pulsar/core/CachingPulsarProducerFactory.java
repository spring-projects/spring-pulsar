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

package org.springframework.pulsar.core;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.cache.provider.CacheProvider;
import org.springframework.pulsar.cache.provider.CacheProviderFactory;
import org.springframework.util.Assert;

/**
 * A {@link PulsarProducerFactory} that extends the {@link DefaultPulsarProducerFactory
 * default implementation} by caching the created producers.
 * <p>
 * The created producer is wrapped in a proxy so that calls to {@link Producer#close()} do
 * not actually close it. The actual close occurs when the producer is evicted from the
 * cache or when {@link DisposableBean#destroy()} is invoked.
 * <p>
 * The proxied producer is cached in an LRU fashion and evicted when it has not been used
 * within a configured time period.
 *
 * @param <T> producer type.
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 */
public class CachingPulsarProducerFactory<T> extends DefaultPulsarProducerFactory<T>
		implements RestartableComponentSupport {

	private static final int LIFECYCLE_PHASE = (Integer.MIN_VALUE / 2) - 100;

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final CacheProvider<ProducerCacheKey<T>, Producer<T>> producerCache;

	private final AtomicReference<State> currentState = RestartableComponentSupport.initialState();

	/**
	 * Construct a caching producer factory with the specified values for the cache
	 * configuration.
	 * @param pulsarClient the client used to create the producers
	 * @param defaultTopic the default topic to use for the producers
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * created producers
	 * @param topicResolver the topic resolver to use
	 * @param cacheExpireAfterAccess time period to expire unused entries in the cache
	 * @param cacheMaximumSize maximum size of cache (entries)
	 * @param cacheInitialCapacity the initial size of cache
	 */
	public CachingPulsarProducerFactory(PulsarClient pulsarClient, @Nullable String defaultTopic,
			List<ProducerBuilderCustomizer<T>> defaultConfigCustomizers, TopicResolver topicResolver,
			Duration cacheExpireAfterAccess, Long cacheMaximumSize, Integer cacheInitialCapacity) {
		super(pulsarClient, defaultTopic, defaultConfigCustomizers, topicResolver);
		var cacheFactory = CacheProviderFactory.<ProducerCacheKey<T>, Producer<T>>load();
		this.producerCache = cacheFactory.create(cacheExpireAfterAccess, cacheMaximumSize, cacheInitialCapacity,
				(key, producer, cause) -> {
					this.logger.debug(() -> "Producer %s evicted from cache due to %s"
						.formatted(ProducerUtils.formatProducer(producer), cause));
					closeProducer(producer, true);
				});
	}

	@Override
	protected Producer<T> doCreateProducer(Schema<T> schema, @Nullable String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		var resolveTopicName = resolveTopicName(topic);
		var producerCacheKey = new ProducerCacheKey<>(schema, resolveTopicName,
				encryptionKeys == null ? null : new HashSet<>(encryptionKeys), customizers);
		return this.producerCache.getOrCreateIfAbsent(producerCacheKey,
				(st) -> createCacheableProducer(st.schema, st.topic, st.encryptionKeys, customizers));
	}

	private Producer<T> createCacheableProducer(Schema<T> schema, String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		var producer = super.doCreateProducer(schema, topic, encryptionKeys, customizers);
		return new ProducerWithCloseCallback<>(producer,
				(p) -> this.logger.trace(() -> "Client closed producer %s but will skip actual closing"
					.formatted(ProducerUtils.formatProducer(producer))));
	}

	/**
	 * Return the phase that this lifecycle object is supposed to run in.
	 * <p>
	 * Because this object depends on the restartable client, it uses a phase slightly
	 * larger than the one used by the restartable client. This ensures that it starts
	 * after and stops before the restartable client.
	 * @return the phase to execute in (just after the restartable client)
	 * @see PulsarClientProxy#getPhase()
	 */
	@Override
	public int getPhase() {
		return LIFECYCLE_PHASE;
	}

	@Override
	public AtomicReference<State> currentState() {
		return this.currentState;
	}

	@Override
	public LogAccessor logger() {
		return this.logger;
	}

	@Override
	public void doStop() {
		this.producerCache.invalidateAll((key, producer) -> closeProducer(producer, false));
	}

	private void closeProducer(Producer<T> producer, boolean async) {
		Producer<T> actualProducer = null;
		if (producer instanceof ProducerWithCloseCallback<T> wrappedProducer) {
			actualProducer = wrappedProducer.getActualProducer();
		}
		if (actualProducer == null) {
			this.logger.trace(() -> "Unable to get actual producer for %s - will skip closing it"
				.formatted(ProducerUtils.formatProducer(producer)));
			return;
		}
		if (async) {
			ProducerUtils.closeProducerAsync(actualProducer, this.logger);
		}
		else {
			ProducerUtils.closeProducer(actualProducer, this.logger, Duration.ofSeconds(15L));
		}
	}

	/**
	 * Uniquely identifies a producer that was handed out by the factory.
	 *
	 * @param <T> type of the schema
	 */
	static class ProducerCacheKey<T> {

		private static final SchemaHash AUTO_PRODUCE_SCHEMA_HASH = SchemaHash.of(new byte[0], SchemaType.AUTO_PUBLISH);

		private final Schema<T> schema;

		private final SchemaHash schemaHash;

		private final String topic;

		@Nullable
		private final Set<String> encryptionKeys;

		@Nullable
		private final List<ProducerBuilderCustomizer<T>> customizers;

		/**
		 * Constructs an instance.
		 * @param schema the schema the producer is configured to use
		 * @param topic the topic the producer is configured to send to
		 * @param encryptionKeys the encryption keys used by the producer
		 * @param customizers the list of producer customizers the producer is configured
		 * to use
		 */
		ProducerCacheKey(Schema<T> schema, String topic, @Nullable Set<String> encryptionKeys,
				@Nullable List<ProducerBuilderCustomizer<T>> customizers) {
			Assert.notNull(schema, () -> "'schema' must be non-null");
			Assert.notNull(topic, () -> "'topic' must be non-null");
			this.schema = schema;
			this.schemaHash = (schema instanceof AutoProduceBytesSchema) ? AUTO_PRODUCE_SCHEMA_HASH
					: SchemaHash.of(this.schema);
			this.topic = topic;
			this.encryptionKeys = encryptionKeys;
			this.customizers = customizers;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			var that = (ProducerCacheKey<?>) o;
			return this.topic.equals(that.topic) && this.schemaHash.equals(that.schemaHash)
					&& Objects.equals(this.encryptionKeys, that.encryptionKeys)
					&& Objects.equals(this.customizers, that.customizers);
		}

		@Override
		public int hashCode() {
			return this.topic.hashCode() + this.schemaHash.hashCode() + Objects.hashCode(this.encryptionKeys)
					+ Objects.hashCode(this.customizers);
		}

		@Override
		public String toString() {
			return "ProducerCacheKey{" + "schema=" + this.schema + ", topic='" + this.topic + '\'' + ", encryptionKeys="
					+ this.encryptionKeys + ", customizers=" + this.customizers + '}';
		}

	}

	/**
	 * A producer that does not actually close when the user calls
	 * {@link Producer#close()}.
	 *
	 * @param <T> producer type.
	 */
	static class ProducerWithCloseCallback<T> implements Producer<T> {

		private final Producer<T> producer;

		private final Consumer<Producer<T>> closeCallback;

		ProducerWithCloseCallback(Producer<T> producer, Consumer<Producer<T>> closeCallback) {
			this.producer = producer;
			this.closeCallback = closeCallback;
		}

		public Producer<T> getActualProducer() {
			return this.producer;
		}

		@Override
		public String getTopic() {
			return this.producer.getTopic();
		}

		@Override
		public String getProducerName() {
			return this.producer.getProducerName();
		}

		@Override
		public MessageId send(T message) throws PulsarClientException {
			return this.producer.send(message);
		}

		@Override
		public CompletableFuture<MessageId> sendAsync(T message) {
			return this.producer.sendAsync(message);
		}

		@Override
		public void flush() throws PulsarClientException {
			this.producer.flush();
		}

		@Override
		public CompletableFuture<Void> flushAsync() {
			return this.producer.flushAsync();
		}

		@Override
		public TypedMessageBuilder<T> newMessage() {
			return this.producer.newMessage();
		}

		@Override
		public <V> TypedMessageBuilder<V> newMessage(Schema<V> schema) {
			return this.producer.newMessage(schema);
		}

		@Override
		public TypedMessageBuilder<T> newMessage(Transaction txn) {
			return this.producer.newMessage(txn);
		}

		@Override
		public long getLastSequenceId() {
			return this.producer.getLastSequenceId();
		}

		@Override
		public ProducerStats getStats() {
			return this.producer.getStats();
		}

		@Override
		public void close() throws PulsarClientException {
			this.closeCallback.accept(this.producer);
		}

		@Override
		public CompletableFuture<Void> closeAsync() {
			this.closeCallback.accept(this.producer);
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public boolean isConnected() {
			return this.producer.isConnected();
		}

		@Override
		public long getLastDisconnectedTimestamp() {
			return this.producer.getLastDisconnectedTimestamp();
		}

		@Override
		public int getNumOfPartitions() {
			return this.producer.getNumOfPartitions();
		}

	}

}

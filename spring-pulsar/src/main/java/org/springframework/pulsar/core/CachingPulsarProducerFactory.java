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

package org.springframework.pulsar.core;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.schema.SchemaHash;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;

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
public class CachingPulsarProducerFactory<T> extends DefaultPulsarProducerFactory<T> implements DisposableBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final Cache<ProducerCacheKey<T>, Producer<T>> producerCache;

	/**
	 * Construct a caching producer factory with the specified values for the cache
	 * configuration.
	 * @param pulsarClient the client used to create the producers
	 * @param producerConfig the configuration to use when creating a producer
	 * @param cacheExpireAfterAccess time period to expire unused entries in the cache
	 * @param cacheMaximumSize maximum size of cache (entries)
	 * @param cacheInitialCapacity the initial size of cache
	 */
	public CachingPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> producerConfig,
			Duration cacheExpireAfterAccess, Long cacheMaximumSize, Integer cacheInitialCapacity) {
		super(pulsarClient, producerConfig);
		this.producerCache = Caffeine.newBuilder().expireAfterAccess(cacheExpireAfterAccess)
				.maximumSize(cacheMaximumSize).initialCapacity(cacheInitialCapacity)
				.scheduler(Scheduler.systemScheduler()).evictionListener(
						(RemovalListener<ProducerCacheKey<T>, Producer<T>>) (producerCacheKey, producer, cause) -> {
							this.logger.debug(() -> String.format("Producer %s evicted from cache due to %s",
									ProducerUtils.formatProducer(producer), cause));
							closeProducer(producer);
						})
				.build();
	}

	@Override
	protected Producer<T> doCreateProducer(Schema<T> schema, @Nullable String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		final String topicName = ProducerUtils.resolveTopicName(topic, this);
		ProducerCacheKey<T> producerCacheKey = new ProducerCacheKey<>(schema, topicName,
				encryptionKeys == null ? null : new HashSet<>(encryptionKeys), customizers);
		return this.producerCache.get(producerCacheKey,
				(st) -> createCacheableProducer(st.schema, st.topic, st.encryptionKeys, customizers));
	}

	private Producer<T> createCacheableProducer(Schema<T> schema, String topic,
			@Nullable Collection<String> encryptionKeys, @Nullable List<ProducerBuilderCustomizer<T>> customizers) {
		try {
			Producer<T> producer = super.doCreateProducer(schema, topic, encryptionKeys, customizers);
			return wrapProducerWithCloseCallback(producer,
					(p) -> this.logger
							.trace(() -> String.format("Client closed producer %s but will skip actual closing",
									ProducerUtils.formatProducer(producer))));
		}
		catch (PulsarClientException ex) {
			throw new RuntimeException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	private Producer<T> wrapProducerWithCloseCallback(Producer<T> producer, Consumer<Producer<T>> closeCallback) {
		ProxyFactory factory = new ProxyFactory(producer);
		factory.addAdvice(new MethodInterceptor() {
			@Nullable
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("close")) {
					closeCallback.accept((Producer<T>) invocation.getThis());
					return null;
				}
				if (invocation.getMethod().getName().equals("closeAsync")) {
					closeCallback.accept((Producer<T>) invocation.getThis());
					return CompletableFuture.completedFuture(null);
				}
				return invocation.proceed();
			}
		});
		return (Producer<T>) factory.getProxy();
	}

	@Override
	public void destroy() {
		this.producerCache.asMap().forEach((producerCacheKey, producer) -> {
			this.producerCache.invalidate(producerCacheKey);
			closeProducer(producer);
		});
	}

	@SuppressWarnings("unchecked")
	private void closeProducer(Producer<T> producer) {
		Producer<T> actualProducer = (Producer<T>) AopProxyUtils.getSingletonTarget(producer);
		if (actualProducer == null) {
			this.logger.warn(() -> String.format("Unable to get actual producer for %s - will skip closing it",
					ProducerUtils.formatProducer(producer)));
			return;
		}
		ProducerUtils.closeProducerAsync(actualProducer, this.logger);
	}

	/**
	 * Uniquely identifies a producer that was handed out by the factory.
	 *
	 * @param <T> type of the schema
	 */
	static class ProducerCacheKey<T> {

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
			this.schemaHash = SchemaHash.of(this.schema);
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
			ProducerCacheKey<?> that = (ProducerCacheKey<?>) o;
			return this.topic.equals(that.topic) && this.schemaHash.equals(that.schemaHash)
					&& Objects.equals(this.encryptionKeys, that.encryptionKeys)
					&& Objects.equals(this.customizers, that.customizers);
		}

		@Override
		public int hashCode() {
			return this.topic.hashCode() + this.schemaHash.hashCode() + Objects.hashCode(this.encryptionKeys)
					+ Objects.hashCode(this.customizers);
		}

	}

}

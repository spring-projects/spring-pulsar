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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;

/**
 * A {@link PulsarProducerFactory} that extends the {@link DefaultPulsarProducerFactory default implementation}
 * by caching the created producers.
 * <p>
 * The created producer is wrapped in a proxy so that calls to {@link Producer#close()} do not actually close it.
 * The actual close occurs when the producer is evicted from the cache or when {@link DisposableBean#destroy()} is
 * invoked.
 * <p>
 * The proxied producer is cached in an LRU fashion and evicted when it has not been used within a configured time
 * period.
 *
 * @param <T> producer type.
 *
 * @author Chris Bono
 */
public class CachingPulsarProducerFactory<T> extends DefaultPulsarProducerFactory<T> implements DisposableBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final Cache<SchemaTopic<T>, Producer<T>> producerCache;

	/**
	 * Construct a caching producer factory with the specified values for the cache configuration.
	 *
	 * @param pulsarClient the client used to create the producers
	 * @param producerConfig the configuration to use when creating a producer
	 * @param cacheExpireAfterAccess time period to expire unused entries in the cache
	 * @param cacheMaximumSize maximum size of cache (entries)
	 * @param cacheInitialCapacity the initial size of cache
	 */
	public CachingPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> producerConfig,
			Duration cacheExpireAfterAccess, Long cacheMaximumSize, Integer cacheInitialCapacity) {
		super(pulsarClient, producerConfig);
		this.producerCache = Caffeine.newBuilder()
				.expireAfterAccess(cacheExpireAfterAccess)
				.maximumSize(cacheMaximumSize)
				.initialCapacity(cacheInitialCapacity)
				.scheduler(Scheduler.systemScheduler())
				.evictionListener((RemovalListener<SchemaTopic<T>, Producer<T>>) (schemaTopic, producer, cause) -> {
					this.logger.debug(() -> String.format("Producer %s evicted from cache due to %s",
							ProducerUtils.formatProducer(producer), cause));
					closeProducer(producer);
				}).build();
	}

	@Override
	public Producer<T> createProducer(String topic, Schema<T> schema, MessageRouter messageRouter) {
		final String topicName = ProducerUtils.resolveTopicName(topic, this);
		SchemaTopic<T> schemaTopic = new SchemaTopic<>(schema, topicName, messageRouter);
		return this.producerCache.get(schemaTopic, (st) -> {
			try {
				return this.doCreateProducer(st.topicName, st.schema, messageRouter);
			}
			catch (PulsarClientException ex) {
				throw new RuntimeException(ex);
			}
		});
	}

	@Override
	protected Producer<T> doCreateProducer(String topic, Schema<T> schema, MessageRouter messageRouter) throws PulsarClientException {
		Producer<T> producer = super.doCreateProducer(topic, schema, messageRouter);
		return wrapProducerWithCloseCallback(producer, (p) -> this.logger.trace(() ->
					String.format("Client closed producer %s but will skip actual closing", ProducerUtils.formatProducer(producer))));
	}

	@SuppressWarnings("unchecked")
	private Producer<T> wrapProducerWithCloseCallback(Producer<T> producer, Consumer<Producer<T>> closeCallback) {
		ProxyFactory factory = new ProxyFactory(producer);
		factory.addAdvice(new MethodInterceptor() {
			@Nullable
			@Override
			public Object invoke(@Nonnull MethodInvocation invocation) throws Throwable {
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
		this.producerCache.asMap().forEach((schemaTopic, producer) -> {
			this.producerCache.invalidate(schemaTopic);
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
	 * Holder for a schema, topic and optional message router used as unique identifier of a producer in cache key.
	 *
	 * @param schema schema of the message
	 * @param topicName topic to send the message to
	 * @param messageRouter router to use to send the topic
	 *
	 * @param <T> type of the schema
	 */
	record SchemaTopic<T>(Schema<T> schema, String topicName, MessageRouter messageRouter) {
		public SchemaTopic {
			Assert.notNull(schema, () -> "'schema' must be non-null");
			Assert.notNull(topicName, () -> "'topicName' must be non-null");
		}
	}
}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarProducerFactory}.
 *
 * @param <T> producer type.
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
public class DefaultPulsarProducerFactory<T> implements PulsarProducerFactory<T> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final Map<String, Object> producerConfig = new HashMap<>();

	private final PulsarClient pulsarClient;

	public DefaultPulsarProducerFactory(PulsarClient pulsarClient, Map<String, Object> config) {
		this.pulsarClient = pulsarClient;
		if (!CollectionUtils.isEmpty(config)) {
			this.producerConfig.putAll(config);
		}
	}

	@Override
	public Producer<T> createProducer(@Nullable String topic, Schema<T> schema) throws PulsarClientException {
		return doCreateProducer(topic, schema, null, null, null);
	}

	@Override
	public Producer<T> createProducer(@Nullable String topic, Schema<T> schema, @Nullable MessageRouter messageRouter)
			throws PulsarClientException {
		return doCreateProducer(topic, schema, messageRouter, null, null);
	}

	@Override
	public Producer<T> createProducer(@Nullable String topic, Schema<T> schema, @Nullable MessageRouter messageRouter,
			@Nullable List<ProducerInterceptor> producerInterceptors) throws PulsarClientException {
		return doCreateProducer(topic, schema, messageRouter, producerInterceptors, null);
	}

	@Override
	public Producer<T> createProducer(@Nullable String topic, Schema<T> schema, @Nullable MessageRouter messageRouter,
			@Nullable List<ProducerInterceptor> producerInterceptors,
			@Nullable List<ProducerBuilderCustomizer<T>> producerBuilderCustomizers) throws PulsarClientException {
		return doCreateProducer(topic, schema, messageRouter, producerInterceptors, producerBuilderCustomizers);
	}

	/**
	 * Create the actual producer.
	 * @param topic the topic the producer will send messages to or {@code null} to use
	 * the default topic
	 * @param schema the schema of the messages to be sent
	 * @param messageRouter the optional message router to use
	 * @param producerInterceptors the optional producer interceptors to use
	 * @param producerBuilderCustomizers the optional list of customizers to apply to the
	 * producer builder
	 * @return the created producer
	 * @throws PulsarClientException if any error occurs
	 */
	protected Producer<T> doCreateProducer(@Nullable String topic, Schema<T> schema,
			@Nullable MessageRouter messageRouter, @Nullable List<ProducerInterceptor> producerInterceptors,
			@Nullable List<ProducerBuilderCustomizer<T>> producerBuilderCustomizers) throws PulsarClientException {
		final String resolvedTopic = ProducerUtils.resolveTopicName(topic, this);
		this.logger.trace(() -> String.format("Creating producer for '%s' topic", resolvedTopic));
		final ProducerBuilder<T> producerBuilder = this.pulsarClient.newProducer(schema);
		if (!CollectionUtils.isEmpty(this.producerConfig)) {
			producerBuilder.loadConf(this.producerConfig);
		}
		producerBuilder.topic(resolvedTopic);
		if (messageRouter != null) {
			producerBuilder.messageRouter(messageRouter);
		}
		if (!CollectionUtils.isEmpty(producerInterceptors)) {
			producerBuilder.intercept(producerInterceptors.toArray(new ProducerInterceptor[0]));
		}
		if (!CollectionUtils.isEmpty(producerBuilderCustomizers)) {
			producerBuilderCustomizers.forEach((c) -> c.customize(producerBuilder));
		}
		return producerBuilder.create();
	}

	@Override
	public Map<String, Object> getProducerConfig() {
		return this.producerConfig;
	}

}

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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * Template implementation for publishing to Pulsar topics.
 *
 * @param <T> message type.
 *
 * @author Soby Chacko
 */
public class PulsarTemplate<T> {

	private final Map<SchemaTopic, Producer<T>> producerCache = new ConcurrentHashMap<>();

	private final PulsarProducerFactory<T> pulsarProducerFactory;

	private String defaultTopicName;

	private Schema<T> schema;

	public PulsarTemplate(PulsarProducerFactory<T> pulsarProducerFactory) {
		this.pulsarProducerFactory = pulsarProducerFactory;
	}

	public MessageId send(T message) throws PulsarClientException {
		final Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		final SchemaTopic schemaTopic = getSchemaTopic(schema, this.pulsarProducerFactory, null);
		Producer<T> producer = this.producerCache.get(schemaTopic);
		if (producer == null) {
			producer = this.pulsarProducerFactory.createProducer(schema);
			this.producerCache.put(schemaTopic, producer);
		}
		return producer.send(message);
	}

	public CompletableFuture<MessageId> sendAsync(T message) throws PulsarClientException {
		final Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		final SchemaTopic schemaTopic = getSchemaTopic(schema, this.pulsarProducerFactory, null);
		Producer<T> producer = this.producerCache.get(schemaTopic);
		if (producer == null) {
			producer = this.pulsarProducerFactory.createProducer(schema);
			this.producerCache.put(schemaTopic, producer);
		}
		return producer.sendAsync(message);
	}

	public CompletableFuture<MessageId> sendAsync(T message, MessageRouter messageRouter) throws PulsarClientException {
		final Schema<T> schema = this.schema != null ? this.schema : SchemaUtils.getSchema(message);
		final SchemaTopic schemaTopic = getSchemaTopic(schema, this.pulsarProducerFactory, messageRouter);
		Producer<T> producer = this.producerCache.get(schemaTopic);
		if (producer == null) {
			producer = this.pulsarProducerFactory.createProducer(schema, messageRouter);
			this.producerCache.put(schemaTopic, producer);
		}
		return producer.sendAsync(message);
	}

	private SchemaTopic getSchemaTopic(Schema<T> schema, PulsarProducerFactory<T> pulsarProducerFactory, MessageRouter messageRouter) {
		return new SchemaTopic(schema, (String) pulsarProducerFactory.getProducerConfig().get("topicName"), messageRouter);
	}

	public void setDefaultTopicName(String defaultTopicName) {
		this.defaultTopicName = defaultTopicName;
		this.pulsarProducerFactory.getProducerConfig().put("topicName", defaultTopicName);
	}

	public Schema<T> getSchema() {
		return this.schema;
	}

	public void setSchema(Schema<T> schema) {
		this.schema = schema;
	}

	private class SchemaTopic {

		final Schema<T> schema;
		final String topicName;
		final MessageRouter messageRouter;

		SchemaTopic(Schema<T> schema, String topicName, MessageRouter messageRouter) {
			this.schema = schema;
			this.topicName = topicName;
			this.messageRouter = messageRouter;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			@SuppressWarnings("unchecked")
			SchemaTopic that = (SchemaTopic) o;
			if (this.messageRouter == null && that.messageRouter == null) {
				return this.schema.equals(that.schema) && this.topicName.equals(that.topicName);
			}
			else if (this.messageRouter == null) {
				return false;
			}
			else if (that.messageRouter == null) {
				return false;
			}
			return this.schema.equals(that.schema) && this.topicName.equals(that.topicName) && this.messageRouter.equals(that.messageRouter);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.schema, this.topicName, this.messageRouter);
		}
	}
}

/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.reader;

import java.time.Duration;
import java.util.List;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.util.Assert;

/**
 * Container properties for Pulsar {@link Reader}.
 *
 * @author Soby Chacko
 */
public class PulsarReaderContainerProperties {

	private static final Duration DEFAULT_READER_START_TIMEOUT = Duration.ofSeconds(30);

	private Duration readerStartTimeout = DEFAULT_READER_START_TIMEOUT;

	private Object readerListener;

	private AsyncTaskExecutor readerTaskExecutor;

	private List<String> topics;

	private MessageId startMessageId;

	private Schema<?> schema;

	private SchemaType schemaType;

	private SchemaResolver schemaResolver;

	private String subscriptionName;

	public Object getReaderListener() {
		return this.readerListener;
	}

	public PulsarReaderContainerProperties() {
		this.schemaResolver = new DefaultSchemaResolver();
	}

	public void setReaderListener(Object readerListener) {
		this.readerListener = readerListener;
	}

	public AsyncTaskExecutor getReaderTaskExecutor() {
		return this.readerTaskExecutor;
	}

	public void setReaderTaskExecutor(AsyncTaskExecutor readerExecutor) {
		this.readerTaskExecutor = readerExecutor;
	}

	public Duration getReaderStartTimeout() {
		return this.readerStartTimeout;
	}

	/**
	 * Set the timeout to wait for a reader thread to start before logging an error.
	 * Default 30 seconds.
	 * @param readerStartTimeout the reader start timeout.
	 */
	public void setReaderStartTimeout(Duration readerStartTimeout) {
		Assert.notNull(readerStartTimeout, "'readerStartTimeout' cannot be null");
		this.readerStartTimeout = readerStartTimeout;
	}

	public List<String> getTopics() {
		return this.topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

	public MessageId getStartMessageId() {
		return this.startMessageId;
	}

	public void setStartMessageId(MessageId startMessageId) {
		this.startMessageId = startMessageId;
	}

	public Schema<?> getSchema() {
		return this.schema;
	}

	public void setSchema(Schema<?> schema) {
		this.schema = schema;
	}

	public String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
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

}

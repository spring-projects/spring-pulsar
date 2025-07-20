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

package org.springframework.pulsar.reader;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.pulsar.config.StartupFailurePolicy;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Container properties for Pulsar {@link Reader}.
 *
 * @author Soby Chacko
 */
@NullUnmarked
public class PulsarReaderContainerProperties {

	private static final Duration DEFAULT_READER_START_TIMEOUT = Duration.ofSeconds(30);

	private Duration readerStartTimeout = DEFAULT_READER_START_TIMEOUT;

	private @Nullable Object readerListener;

	private @Nullable AsyncTaskExecutor readerTaskExecutor;

	private @Nullable List<String> topics;

	private @Nullable MessageId startMessageId;

	private @Nullable Schema<?> schema;

	private @Nullable SchemaType schemaType;

	private SchemaResolver schemaResolver;

	private @Nullable RetryTemplate startupFailureRetryTemplate;

	private final RetryTemplate defaultStartupFailureRetryTemplate = RetryTemplate.builder()
		.maxAttempts(3)
		.fixedBackoff(Duration.ofSeconds(10))
		.build();

	private StartupFailurePolicy startupFailurePolicy = StartupFailurePolicy.STOP;

	public @Nullable Object getReaderListener() {
		return this.readerListener;
	}

	public PulsarReaderContainerProperties() {
		this.schemaResolver = new DefaultSchemaResolver();
	}

	public void setReaderListener(Object readerListener) {
		this.readerListener = readerListener;
	}

	public @Nullable AsyncTaskExecutor getReaderTaskExecutor() {
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

	public @Nullable List<String> getTopics() {
		return this.topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

	public @Nullable MessageId getStartMessageId() {
		return this.startMessageId;
	}

	public void setStartMessageId(MessageId startMessageId) {
		this.startMessageId = startMessageId;
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

	public SchemaResolver getSchemaResolver() {
		return this.schemaResolver;
	}

	public void setSchemaResolver(SchemaResolver schemaResolver) {
		this.schemaResolver = schemaResolver;
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
	public void setStartupFailureRetryTemplate(RetryTemplate startupFailureRetryTemplate) {
		this.startupFailureRetryTemplate = startupFailureRetryTemplate;
		if (this.startupFailureRetryTemplate != null) {
			setStartupFailurePolicy(StartupFailurePolicy.RETRY);
		}
	}

	public StartupFailurePolicy getStartupFailurePolicy() {
		return this.startupFailurePolicy;
	}

	/**
	 * The action to take on the container when a failure occurs during startup.
	 * @param startupFailurePolicy action to take when a failure occurs during startup
	 * @since 1.2.0
	 */
	public void setStartupFailurePolicy(StartupFailurePolicy startupFailurePolicy) {
		this.startupFailurePolicy = Objects.requireNonNull(startupFailurePolicy,
				"startupFailurePolicy must not be null");
	}

}

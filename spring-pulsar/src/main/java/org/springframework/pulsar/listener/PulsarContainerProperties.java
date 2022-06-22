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

package org.springframework.pulsar.listener;

import java.time.Duration;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.util.Assert;

/**
 * @author Soby Chacko
 */
public class PulsarContainerProperties extends PulsarConsumerProperties {

	private static final Duration DEFAULT_CONSUMER_START_TIMEOUT = Duration.ofSeconds(30);

	private Duration consumerStartTimeout = DEFAULT_CONSUMER_START_TIMEOUT;

	public enum AckMode {

		MANUAL;
	}

	private Schema<?> schema = Schema.BYTES;

	private Object messageListener;
	private AsyncListenableTaskExecutor consumerTaskExecutor;
	private SubscriptionType subscriptionType;

	private int maxNumMessages = -1;
	private int maxNumBytes = 10 * 1024 * 1024;
	private int batchTimeout = 100;

	private boolean batchReceive;
	private boolean batchAsyncReceive;

	private boolean asyncReceive;

	private AckMode ackMode;

	public PulsarContainerProperties(String... topics) {
		super(topics);
	}

	public Object getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(Object messageListener) {
		this.messageListener = messageListener;
	}

	public AsyncListenableTaskExecutor getConsumerTaskExecutor() {
		return this.consumerTaskExecutor;
	}

	public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerExecutor) {
		this.consumerTaskExecutor = consumerExecutor;
	}

	public SubscriptionType getSubscriptionType() {
		return subscriptionType;
	}

	public void setSubscriptionType(SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public int getMaxNumMessages() {
		return maxNumMessages;
	}

	public void setMaxNumMessages(int maxNumMessages) {
		this.maxNumMessages = maxNumMessages;
	}

	public int getMaxNumBytes() {
		return maxNumBytes;
	}

	public void setMaxNumBytes(int maxNumBytes) {
		this.maxNumBytes = maxNumBytes;
	}

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public boolean isBatchReceive() {
		return batchReceive;
	}

	public void setBatchReceive(boolean batchReceive) {
		this.batchReceive = batchReceive;
	}

	public boolean isBatchAsyncReceive() {
		return batchAsyncReceive;
	}

	public void setBatchAsyncReceive(boolean batchAsyncReceive) {
		this.batchAsyncReceive = batchAsyncReceive;
	}

	public boolean isAsyncReceive() {
		return asyncReceive;
	}

	public void setAsyncReceive(boolean asyncReceive) {
		this.asyncReceive = asyncReceive;
	}

	public AckMode getAckMode() {
		return ackMode;
	}

	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	public Duration getConsumerStartTimeout() {
		return this.consumerStartTimeout;
	}

	/**
	 * Set the timeout to wait for a consumer thread to start before logging
	 * an error. Default 30 seconds.
	 * @param consumerStartTimeout the consumer start timeout.
	 */
	public void setConsumerStartTimeout(Duration consumerStartTimeout) {
		Assert.notNull(consumerStartTimeout, "'consumerStartTimout' cannot be null");
		this.consumerStartTimeout = consumerStartTimeout;
	}

	public Schema<?> getSchema() {
		return schema;
	}

	public void setSchema(Schema<?> schema) {
		this.schema = schema;
	}
}

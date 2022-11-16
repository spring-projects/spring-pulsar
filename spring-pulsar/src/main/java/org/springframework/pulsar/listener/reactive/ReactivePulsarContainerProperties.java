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

package org.springframework.pulsar.listener.reactive;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Contains runtime properties for a reactive listener container.
 *
 * @param <T> message type.
 * @author Christophe Bornet
 */
public class ReactivePulsarContainerProperties<T> {

	private List<String> topics;

	private Pattern topicsPattern;

	private String subscriptionName;

	private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

	private Schema<T> schema;

	private ReactivePulsarMessageHandler messageHandler;

	private Duration handlingTimeout = Duration.ofMinutes(2);

	private int concurrency = 0;

	private int maxInFlight = 0;

	private boolean useKeyOrderedProcessing = false;

	public ReactivePulsarMessageHandler getMessageHandler() {
		return this.messageHandler;
	}

	public void setMessageHandler(ReactivePulsarMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	public SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public Schema<T> getSchema() {
		return this.schema;
	}

	public void setSchema(Schema<T> schema) {
		this.schema = schema;
	}

	public List<String> getTopics() {
		return this.topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

	public Pattern getTopicsPattern() {
		return this.topicsPattern;
	}

	public void setTopicsPattern(Pattern topicsPattern) {
		this.topicsPattern = topicsPattern;
	}

	public void setTopicsPattern(String topicsPattern) {
		this.topicsPattern = Pattern.compile(topicsPattern);
	}

	public String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public Duration getHandlingTimeout() {
		return this.handlingTimeout;
	}

	public void setHandlingTimeout(Duration handlingTimeout) {
		this.handlingTimeout = handlingTimeout;
	}

	public int getConcurrency() {
		return this.concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public int getMaxInFlight() {
		return this.maxInFlight;
	}

	public void setMaxInFlight(int maxInFlight) {
		this.maxInFlight = maxInFlight;
	}

	public boolean isUseKeyOrderedProcessing() {
		return this.useKeyOrderedProcessing;
	}

	public void setUseKeyOrderedProcessing(boolean useKeyOrderedProcessing) {
		this.useKeyOrderedProcessing = useKeyOrderedProcessing;
	}

}

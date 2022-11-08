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

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Contains runtime properties for a reactive listener container.
 *
 * @author Christophe Bornet
 */
public class ReactivePulsarContainerProperties {

	private String[] topics;

	private String topicsPattern;

	private String subscriptionName;

	private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

	private Schema<?> schema;

	private SchemaType schemaType;

	private Object messageHandler;

	private long handlingTimeoutMillis = TimeUnit.MINUTES.toMillis(2);

	private int concurrency = 0;

	private int maxInFlight = 0;

	public ReactivePulsarContainerProperties(String... topics) {
		this.topics = topics.clone();
		this.topicsPattern = null;
	}

	public ReactivePulsarContainerProperties(String topicPattern) {
		this.topicsPattern = topicPattern;
		this.topics = null;
	}

	public Object getMessageHandler() {
		return this.messageHandler;
	}

	public void setMessageHandler(Object messageHandler) {
		this.messageHandler = messageHandler;
	}

	public SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public Schema<?> getSchema() {
		return this.schema;
	}

	public void setSchema(Schema<?> schema) {
		this.schema = schema;
	}

	public String[] getTopics() {
		return this.topics;
	}

	public void setTopics(String[] topics) {
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

	public SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	public long getHandlingTimeoutMillis() {
		return this.handlingTimeoutMillis;
	}

	public void setHandlingTimeoutMillis(long handlingTimeoutMillis) {
		this.handlingTimeoutMillis = handlingTimeoutMillis;
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

}

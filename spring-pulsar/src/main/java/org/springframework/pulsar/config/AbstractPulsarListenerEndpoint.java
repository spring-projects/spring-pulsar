/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.pulsar.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;
import org.springframework.pulsar.listener.adapter.PulsarMessagingMessageListenerAdapter;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base implementation for {@link PulsarListenerEndpoint}.
 *
 * @param <K> Message payload type.
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public abstract class AbstractPulsarListenerEndpoint<K>
		implements PulsarListenerEndpoint, BeanFactoryAware, InitializingBean {

	private String subscriptionName;

	private SubscriptionType subscriptionType;

	private SchemaType schemaType;

	private String id;

	private final Collection<String> topics = new ArrayList<>();

	private String topicPattern;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private BeanResolver beanResolver;

	private Boolean autoStartup;

	private Properties consumerProperties;

	private Boolean batchListener;

	private Integer concurrency;

	private AckMode ackMode;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
		this.beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Nullable
	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	@Override
	public void afterPropertiesSet() {
		boolean topicsEmpty = getTopics().isEmpty();
		if (!topicsEmpty && !StringUtils.hasText(getTopicPattern())) {
			throw new IllegalStateException("Topics or topicPattern must be provided but not both for " + this);
		}
	}

	@Nullable
	protected BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	@Nullable
	protected BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

	@Nullable
	protected BeanResolver getBeanResolver() {
		return this.beanResolver;
	}

	public void setSubscriptionName(String subscriptionName) {

		this.subscriptionName = subscriptionName;
	}

	@Nullable
	@Override
	public String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	public void setTopics(String... topics) {
		Assert.notNull(topics, "'topics' must not be null");
		this.topics.clear();
		this.topics.addAll(Arrays.asList(topics));
	}

	@Override
	public Collection<String> getTopics() {
		return Collections.unmodifiableCollection(this.topics);
	}

	public void setTopicPattern(String topicPattern) {
		Assert.notNull(topicPattern, "'topicPattern' must not be null");
		this.topicPattern = topicPattern;
	}

	@Override
	public String getTopicPattern() {
		return this.topicPattern;
	}

	@Override
	@Nullable
	public Boolean getAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void setupListenerContainer(PulsarMessageListenerContainer listenerContainer,
			@Nullable MessageConverter messageConverter) {

		setupMessageListener(listenerContainer, messageConverter);
	}

	@SuppressWarnings("unchecked")
	private void setupMessageListener(PulsarMessageListenerContainer container,
			@Nullable MessageConverter messageConverter) {

		PulsarMessagingMessageListenerAdapter<K> adapter = createMessageListener(container, messageConverter);
		Object messageListener = adapter;
		boolean isBatchListener = isBatchListener();
		Assert.state(messageListener != null, () -> "Endpoint [" + this + "] must provide a non null message listener");
		container.setupMessageListener(messageListener);
	}

	protected abstract PulsarMessagingMessageListenerAdapter<K> createMessageListener(
			PulsarMessageListenerContainer container, @Nullable MessageConverter messageConverter);

	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public Properties getConsumerProperties() {
		return this.consumerProperties;
	}

	@Nullable
	public Boolean getBatchListener() {
		return this.batchListener;
	}

	public void setBatchListener(boolean batchListener) {
		this.batchListener = batchListener;
	}

	public boolean isBatchListener() {
		return this.batchListener == null ? false : this.batchListener;
	}

	public SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	@Override
	@Nullable
	public Integer getConcurrency() {
		return this.concurrency;
	}

	/**
	 * Set the concurrency for this endpoint's container.
	 * @param concurrency the concurrency.
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	public AckMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

}

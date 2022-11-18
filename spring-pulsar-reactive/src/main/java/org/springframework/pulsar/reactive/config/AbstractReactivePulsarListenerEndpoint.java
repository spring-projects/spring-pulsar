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

package org.springframework.pulsar.reactive.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
import org.springframework.pulsar.listener.adapter.PulsarMessagingMessageListenerAdapter;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageHandler;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base implementation for {@link ReactivePulsarListenerEndpoint}.
 *
 * @param <T> Message payload type.
 * @author Christophe Bornet
 */
public abstract class AbstractReactivePulsarListenerEndpoint<T>
		implements ReactivePulsarListenerEndpoint<T>, BeanFactoryAware, InitializingBean {

	private String subscriptionName;

	private SubscriptionType subscriptionType;

	private SchemaType schemaType;

	private String id;

	private Collection<String> topics = new ArrayList<>();

	private String topicPattern;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private BeanResolver beanResolver;

	private Boolean autoStartup;

	private Boolean fluxListener;

	private Integer concurrency;

	private Boolean useKeyOrderedProcessing;

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
		this.topics = Arrays.asList(topics);
	}

	@Override
	public List<String> getTopics() {
		return new ArrayList<>(this.topics);
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
	public void setupListenerContainer(ReactivePulsarMessageListenerContainer<T> listenerContainer,
			@Nullable MessageConverter messageConverter) {

		setupMessageListener(listenerContainer, messageConverter);
	}

	@SuppressWarnings("unchecked")
	private void setupMessageListener(ReactivePulsarMessageListenerContainer<T> container,
			@Nullable MessageConverter messageConverter) {

		PulsarMessagingMessageListenerAdapter<T> adapter = createMessageHandler(container, messageConverter);
		Assert.state(adapter != null, () -> "Endpoint [" + this + "] must provide a non null message handler");
		container.setupMessageHandler((ReactivePulsarMessageHandler) adapter);
	}

	protected abstract PulsarMessagingMessageListenerAdapter<T> createMessageHandler(
			ReactivePulsarMessageListenerContainer<T> container, @Nullable MessageConverter messageConverter);

	@Nullable
	public Boolean getFluxListener() {
		return this.fluxListener;
	}

	public void setFluxListener(boolean fluxListener) {
		this.fluxListener = fluxListener;
	}

	@Override
	public boolean isFluxListener() {
		return this.fluxListener != null && this.fluxListener;
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

	@Override
	public Boolean getUseKeyOrderedProcessing() {
		return this.useKeyOrderedProcessing;
	}

	public void setUseKeyOrderedProcessing(Boolean useKeyOrderedProcessing) {
		this.useKeyOrderedProcessing = useKeyOrderedProcessing;
	}

}

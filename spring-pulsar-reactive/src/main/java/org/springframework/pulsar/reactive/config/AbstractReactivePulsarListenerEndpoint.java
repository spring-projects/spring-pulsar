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

package org.springframework.pulsar.reactive.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.BeanResolver;
import org.springframework.pulsar.listener.adapter.AbstractPulsarMessageToSpringMessageAdapter;
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

	private @Nullable String subscriptionName;

	private @Nullable SubscriptionType subscriptionType;

	private @Nullable SchemaType schemaType;

	private @Nullable String id;

	private Collection<String> topics = new ArrayList<>();

	private @Nullable String topicPattern;

	private @Nullable BeanFactory beanFactory;

	private @Nullable BeanExpressionResolver resolver;

	private @Nullable BeanExpressionContext expressionContext;

	private @Nullable BeanResolver beanResolver;

	private @Nullable Boolean autoStartup;

	private @Nullable Boolean fluxListener;

	private @Nullable Integer concurrency;

	private @Nullable Boolean useKeyOrderedProcessing;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
		this.beanResolver = new BeanFactoryResolver(beanFactory);
	}

	protected @Nullable BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	@Override
	public void afterPropertiesSet() {
		boolean topicsEmpty = getTopics().isEmpty();
		if (!topicsEmpty && !StringUtils.hasText(getTopicPattern())) {
			throw new IllegalStateException("Topics or topicPattern must be provided but not both for " + this);
		}
	}

	protected @Nullable BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	protected @Nullable BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

	protected @Nullable BeanResolver getBeanResolver() {
		return this.beanResolver;
	}

	public void setSubscriptionName(@Nullable String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	@Nullable
	@Override
	public String getSubscriptionName() {
		return this.subscriptionName;
	}

	public void setId(@Nullable String id) {
		this.id = id;
	}

	@Override
	public @Nullable String getId() {
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

	public void setTopicPattern(@Nullable String topicPattern) {
		this.topicPattern = topicPattern;
	}

	@Override
	public @Nullable String getTopicPattern() {
		return this.topicPattern;
	}

	@Override
	public @Nullable Boolean getAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(@Nullable Boolean autoStartup) {
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
		AbstractPulsarMessageToSpringMessageAdapter<T> adapter = createMessageHandler(container, messageConverter);
		Assert.state(adapter != null, () -> "Endpoint [" + this + "] must provide a non null message handler");
		container.setupMessageHandler((ReactivePulsarMessageHandler) adapter);
	}

	protected abstract AbstractPulsarMessageToSpringMessageAdapter<T> createMessageHandler(
			ReactivePulsarMessageListenerContainer<T> container, @Nullable MessageConverter messageConverter);

	public @Nullable Boolean getFluxListener() {
		return this.fluxListener;
	}

	public void setFluxListener(boolean fluxListener) {
		this.fluxListener = fluxListener;
	}

	@Override
	public boolean isFluxListener() {
		return this.fluxListener != null && this.fluxListener;
	}

	public @Nullable SubscriptionType getSubscriptionType() {
		return this.subscriptionType;
	}

	public void setSubscriptionType(@Nullable SubscriptionType subscriptionType) {
		this.subscriptionType = subscriptionType;
	}

	public @Nullable SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(@Nullable SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	@Override
	public @Nullable Integer getConcurrency() {
		return this.concurrency;
	}

	/**
	 * Set the concurrency for this endpoint's container.
	 * @param concurrency the concurrency.
	 */
	public void setConcurrency(@Nullable Integer concurrency) {
		this.concurrency = concurrency;
	}

	@Override
	public @Nullable Boolean getUseKeyOrderedProcessing() {
		return this.useKeyOrderedProcessing;
	}

	public void setUseKeyOrderedProcessing(@Nullable Boolean useKeyOrderedProcessing) {
		this.useKeyOrderedProcessing = useKeyOrderedProcessing;
	}

}

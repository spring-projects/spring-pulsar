/*
 * Copyright 2023-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * Base class for the various container implementations.
 *
 * @author Soby Chacko
 */
public abstract class AbstractPulsarMessageContainer implements ApplicationEventPublisherAware, BeanNameAware,
		ApplicationContextAware, SmartLifecycle, DisposableBean {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private @Nullable String beanName;

	private @Nullable ApplicationContext applicationContext;

	private int phase;

	protected boolean autoStartup = true;

	protected volatile boolean running = false;

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Get the event publisher.
	 * @return the publisher
	 */
	public @Nullable ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Return the bean name.
	 * @return the bean name.
	 */
	public @Nullable String getBeanName() {
		return this.beanName; // the container factory sets this to the listener id
	}

	protected String requireNonNullBeanName() {
		Assert.notNull(this.beanName, "beanName must not be null");
		return this.beanName;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	protected @Nullable ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	protected abstract void doStart();

	protected abstract void doStop();

}

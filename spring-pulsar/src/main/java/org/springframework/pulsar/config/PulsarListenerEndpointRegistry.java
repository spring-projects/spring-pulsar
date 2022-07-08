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

package org.springframework.pulsar.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.listener.AbstractPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarListenerContainerRegistry;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;
import org.springframework.pulsar.support.EndpointHandlerMethod;
import org.springframework.util.Assert;

/**
 * Creates the necessary {@link PulsarMessageListenerContainer} instances for the
 * registered {@linkplain PulsarListenerEndpoint endpoints}. Also manages the
 * lifecycle of the listener containers, in particular within the lifecycle
 * of the application context.
 *
 * <p>Contrary to {@link PulsarMessageListenerContainer}s created manually, listener
 * containers managed by registry are not beans in the application context and
 * are not candidates for autowiring. Use {@link #getListenerContainers()} if
 * you need to access this registry's listener containers for management purposes.
 * If you need to access to a specific message listener container, use
 * {@link #getListenerContainer(String)} with the id of the endpoint.
 *
 * @author Soby Chacko
 */
public class PulsarListenerEndpointRegistry implements PulsarListenerContainerRegistry, DisposableBean, SmartLifecycle,
		ApplicationContextAware, ApplicationListener<ContextRefreshedEvent> {

	private final Map<String, PulsarMessageListenerContainer> listenerContainers = new ConcurrentHashMap<>();

	private ConfigurableApplicationContext applicationContext;

	private int phase = AbstractPulsarMessageListenerContainer.DEFAULT_PHASE;

	private boolean contextRefreshed;


	private volatile boolean running;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (applicationContext instanceof ConfigurableApplicationContext) {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}
	}

	@Override
	@Nullable
	public PulsarMessageListenerContainer getListenerContainer(String id) {
		Assert.hasText(id, "Container identifier must not be empty");
		return this.listenerContainers.get(id);
	}

	@Override
	public Set<String> getListenerContainerIds() {
		return Collections.unmodifiableSet(this.listenerContainers.keySet());
	}

	@Override
	public Collection<PulsarMessageListenerContainer> getListenerContainers() {
		return Collections.unmodifiableCollection(this.listenerContainers.values());
	}

	@Override
	public Collection<PulsarMessageListenerContainer> getAllListenerContainers() {
		List<PulsarMessageListenerContainer> containers = new ArrayList<>();
		containers.addAll(getListenerContainers());
		containers.addAll(this.applicationContext.getBeansOfType(PulsarMessageListenerContainer.class, true, false).values());
		return containers;
	}

	public void registerListenerContainer(PulsarListenerEndpoint endpoint, PulsarListenerContainerFactory<?> factory) {
		registerListenerContainer(endpoint, factory, false);
	}

	public void registerListenerContainer(PulsarListenerEndpoint endpoint, PulsarListenerContainerFactory<?> factory,
										boolean startImmediately) {
		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.notNull(factory, "Factory must not be null");

		String subscriptionName = endpoint.getSubscriptionName();
		String id = endpoint.getId();

		Assert.hasText(subscriptionName, "Endpoint id must not be empty");

		synchronized (this.listenerContainers) {
			Assert.state(!this.listenerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + subscriptionName + "'");
			PulsarMessageListenerContainer container = createListenerContainer(endpoint, factory);
			this.listenerContainers.put(id, container);
			ConfigurableApplicationContext appContext = this.applicationContext;
		}
	}

	protected PulsarMessageListenerContainer createListenerContainer(PulsarListenerEndpoint endpoint,
																	PulsarListenerContainerFactory<?> factory) {

		if (endpoint instanceof MethodPulsarListenerEndpoint) {
			MethodPulsarListenerEndpoint<?> mkle = (MethodPulsarListenerEndpoint<?>) endpoint;
			Object bean = mkle.getBean();
			if (bean instanceof EndpointHandlerMethod) {
				EndpointHandlerMethod ehm = (EndpointHandlerMethod) bean;
				ehm = new EndpointHandlerMethod(ehm.resolveBean(this.applicationContext), ehm.getMethodName());
				mkle.setBean(ehm.resolveBean(this.applicationContext));
				mkle.setMethod(ehm.getMethod());
			}
		}
		PulsarMessageListenerContainer listenerContainer = factory.createListenerContainer(endpoint);

		if (listenerContainer instanceof InitializingBean) {
			try {
				((InitializingBean) listenerContainer).afterPropertiesSet();
			}
			catch (Exception ex) {
				throw new BeanInitializationException("Failed to initialize message listener container", ex);
			}
		}

		int containerPhase = listenerContainer.getPhase();
		if (listenerContainer.isAutoStartup() &&
				containerPhase != AbstractPulsarMessageListenerContainer.DEFAULT_PHASE) {  // a custom phase value
			if (this.phase != AbstractPulsarMessageListenerContainer.DEFAULT_PHASE && this.phase != containerPhase) {
				throw new IllegalStateException("Encountered phase mismatch between container "
						+ "factory definitions: " + this.phase + " vs " + containerPhase);
			}
			this.phase = listenerContainer.getPhase();
		}

		return listenerContainer;
	}

	@Override
	public void destroy() {
		for (PulsarMessageListenerContainer listenerContainer : getListenerContainers()) {
			listenerContainer.destroy();
		}
	}


	// Delegating implementation of SmartLifecycle

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void start() {
		for (PulsarMessageListenerContainer listenerContainer : getListenerContainers()) {
			startIfNecessary(listenerContainer);
		}
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
		for (PulsarMessageListenerContainer listenerContainer : getListenerContainers()) {
			listenerContainer.stop();
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.running = false;
		Collection<PulsarMessageListenerContainer> listenerContainersToStop = getListenerContainers();
		if (listenerContainersToStop.size() > 0) {
			AggregatingCallback aggregatingCallback = new AggregatingCallback(listenerContainersToStop.size(),
					callback);
			for (PulsarMessageListenerContainer listenerContainer : listenerContainersToStop) {
				if (listenerContainer.isRunning()) {
					listenerContainer.stop(aggregatingCallback);
				}
				else {
					aggregatingCallback.run();
				}
			}
		}
		else {
			callback.run();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}


	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			this.contextRefreshed = true;
		}
	}


	private void startIfNecessary(PulsarMessageListenerContainer listenerContainer) {
		if (this.contextRefreshed || listenerContainer.isAutoStartup()) {
			listenerContainer.start();
		}
	}

	private static final class AggregatingCallback implements Runnable {

		private final AtomicInteger count;

		private final Runnable finishCallback;

		private AggregatingCallback(int count, Runnable finishCallback) {
			this.count = new AtomicInteger(count);
			this.finishCallback = finishCallback;
		}

		@Override
		public void run() {
			if (this.count.decrementAndGet() <= 0) {
				this.finishCallback.run();
			}
		}

	}


}

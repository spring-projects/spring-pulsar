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
import org.springframework.pulsar.reader.PulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarReaderContainerRegistry;
import org.springframework.util.Assert;

/**
 * Creates the necessary container instances for the registered
 * {@linkplain PulsarReaderEndpoint endpoints}. Also manages the lifecycle of the reader
 * containers, in particular within the lifecycle of the application context.
 *
 * <p>
 * Contrary to containers created manually, reader listener containers managed by registry
 * are not beans in the application context and are not candidates for autowiring. Use
 * {@link #getReaderContainers()} ()} if you need to access this registry's reader
 * listener containers for management purposes. If you need to access to a specific reader
 * listener container, use {@link #getReaderContainer(String)} with the id of the
 * endpoint.
 *
 * @param <C> container type
 * @param <E> endpoint type
 * @author Soby Chacko
 */
public class GenericReaderEndpointRegistry<C extends PulsarMessageReaderContainer, E extends PulsarReaderEndpoint<C>>
		implements PulsarReaderContainerRegistry, DisposableBean, SmartLifecycle, ApplicationContextAware,
		ApplicationListener<ContextRefreshedEvent> {

	private final Class<? extends C> type;

	private final Map<String, C> readerContainers = new ConcurrentHashMap<>();

	private ConfigurableApplicationContext applicationContext;

	private int phase = C.DEFAULT_PHASE;

	private boolean contextRefreshed;

	private volatile boolean running;

	@SuppressWarnings("unchecked")
	protected GenericReaderEndpointRegistry(Class<?> type) {
		this.type = (Class<? extends C>) type;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (applicationContext instanceof ConfigurableApplicationContext) {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}
	}

	@Override
	@Nullable
	public C getReaderContainer(String id) {
		Assert.hasText(id, "Container identifier must not be empty");
		return this.readerContainers.get(id);
	}

	@Override
	public Set<String> getReaderContainerIds() {
		return Collections.unmodifiableSet(this.readerContainers.keySet());
	}

	@Override
	public Collection<C> getReaderContainers() {
		return Collections.unmodifiableCollection(this.readerContainers.values());
	}

	@Override
	public Collection<C> getAllReaderContainers() {
		List<C> containers = new ArrayList<>(getReaderContainers());
		containers.addAll(this.applicationContext.getBeansOfType(this.type, true, false).values());
		return containers;
	}

	public void registerReaderContainer(E endpoint, ReaderContainerFactory<? extends C, E> factory) {
		registerReaderContainer(endpoint, factory, false);
	}

	public void registerReaderContainer(E endpoint, ReaderContainerFactory<? extends C, E> factory,
			boolean startImmediately) {
		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.notNull(factory, "Factory must not be null");

		String subscriptionName = endpoint.getSubscriptionName();
		String id = endpoint.getId();

		Assert.hasText(subscriptionName, "Endpoint id must not be empty");

		synchronized (this.readerContainers) {
			Assert.state(!this.readerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + subscriptionName + "'");
			C container = createReaderContainer(endpoint, factory);
			this.readerContainers.put(id, container);
		}
	}

	protected C createReaderContainer(E endpoint, ReaderContainerFactory<? extends C, E> factory) {

		C readerContainer = factory.createReaderContainer(endpoint);

		if (readerContainer instanceof InitializingBean) {
			try {
				((InitializingBean) readerContainer).afterPropertiesSet();
			}
			catch (Exception ex) {
				throw new BeanInitializationException("Failed to initialize message listener container", ex);
			}
		}

		int containerPhase = readerContainer.getPhase();
		if (readerContainer.isAutoStartup() && containerPhase != C.DEFAULT_PHASE) { // a
			// custom
			// phase
			// value
			if (this.phase != C.DEFAULT_PHASE && this.phase != containerPhase) {
				throw new IllegalStateException("Encountered phase mismatch between container "
						+ "factory definitions: " + this.phase + " vs " + containerPhase);
			}
			this.phase = readerContainer.getPhase();
		}

		return readerContainer;
	}

	@Override
	public void destroy() throws Exception {
		for (C listenerContainer : getReaderContainers()) {
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
		for (C listenerContainer : getReaderContainers()) {
			startIfNecessary(listenerContainer);
		}
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
		for (C listenerContainer : getReaderContainers()) {
			listenerContainer.stop();
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.running = false;
		Collection<C> readerContainersToStop = getReaderContainers();
		if (readerContainersToStop.size() > 0) {
			GenericReaderEndpointRegistry.AggregatingCallback aggregatingCallback = new GenericReaderEndpointRegistry.AggregatingCallback(
					readerContainersToStop.size(), callback);
			for (C readerContainer : readerContainersToStop) {
				if (readerContainer.isRunning()) {
					readerContainer.stop(aggregatingCallback);
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

	private void startIfNecessary(C listenerContainer) {
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

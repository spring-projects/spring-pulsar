/*
 * Copyright 2022-present the original author or authors.
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
import java.util.concurrent.locks.ReentrantLock;

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
import org.springframework.pulsar.listener.MessageListenerContainer;
import org.springframework.pulsar.listener.PulsarListenerContainerRegistry;
import org.springframework.util.Assert;

/**
 * Creates the necessary container instances for the registered
 * {@linkplain ListenerEndpoint endpoints}. Also manages the lifecycle of the listener
 * containers, in particular within the lifecycle of the application context.
 *
 * <p>
 * Contrary to containers created manually, listener containers managed by registry are
 * not beans in the application context and are not candidates for autowiring. Use
 * {@link #getListenerContainers()} if you need to access this registry's listener
 * containers for management purposes. If you need to access to a specific message
 * listener container, use {@link #getListenerContainer(String)} with the id of the
 * endpoint.
 *
 * @param <C> listener container type.
 * @param <E> listener endpoint type.
 * @author Soby Chacko
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class GenericListenerEndpointRegistry<C extends MessageListenerContainer, E extends ListenerEndpoint<C>>
		implements PulsarListenerContainerRegistry, DisposableBean, SmartLifecycle, ApplicationContextAware,
		ApplicationListener<ContextRefreshedEvent> {

	private final Class<? extends C> type;

	private final Map<String, C> listenerContainers = new ConcurrentHashMap<>();

	private final ReentrantLock containersLock = new ReentrantLock();

	private ConfigurableApplicationContext applicationContext;

	private int phase = C.DEFAULT_PHASE;

	private boolean contextRefreshed;

	private volatile boolean running;

	@SuppressWarnings("unchecked")
	protected GenericListenerEndpointRegistry(Class<?> type) {
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
	public C getListenerContainer(String id) {
		Assert.hasText(id, "Container identifier must not be empty");
		return this.listenerContainers.get(id);
	}

	@Override
	public Set<String> getListenerContainerIds() {
		return Collections.unmodifiableSet(this.listenerContainers.keySet());
	}

	@Override
	public Collection<C> getListenerContainers() {
		return Collections.unmodifiableCollection(this.listenerContainers.values());
	}

	@Override
	public Collection<C> getAllListenerContainers() {
		List<C> containers = new ArrayList<>(getListenerContainers());
		containers.addAll(this.applicationContext.getBeansOfType(this.type, true, false).values());
		return containers;
	}

	public void registerListenerContainer(E endpoint, ListenerContainerFactory<? extends C, E> factory) {
		registerListenerContainer(endpoint, factory, false);
	}

	public void registerListenerContainer(E endpoint, ListenerContainerFactory<? extends C, E> factory,
			boolean startImmediately) {
		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.notNull(factory, "Factory must not be null");
		String id = endpoint.getId();
		Assert.hasText(id, "Endpoint id must not be empty");
		this.containersLock.lock();
		try {
			Assert.state(!this.listenerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + id + "'");
			C container = createListenerContainer(endpoint, factory);
			this.listenerContainers.put(id, container);
		}
		finally {
			this.containersLock.unlock();
		}
	}

	protected C createListenerContainer(E endpoint, ListenerContainerFactory<? extends C, E> factory) {

		C listenerContainer = factory.createRegisteredContainer(endpoint);

		if (listenerContainer instanceof InitializingBean) {
			try {
				((InitializingBean) listenerContainer).afterPropertiesSet();
			}
			catch (Exception ex) {
				throw new BeanInitializationException("Failed to initialize message listener container", ex);
			}
		}

		int containerPhase = listenerContainer.getPhase();
		if (listenerContainer.isAutoStartup() && containerPhase != C.DEFAULT_PHASE) {
			if (this.phase != C.DEFAULT_PHASE && this.phase != containerPhase) {
				throw new IllegalStateException("Encountered phase mismatch between container "
						+ "factory definitions: " + this.phase + " vs " + containerPhase);
			}
			this.phase = listenerContainer.getPhase();
		}

		return listenerContainer;
	}

	@Override
	public void destroy() throws Exception {
		for (C listenerContainer : getListenerContainers()) {
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
		for (C listenerContainer : getListenerContainers()) {
			startIfNecessary(listenerContainer);
		}
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
		for (C listenerContainer : getListenerContainers()) {
			listenerContainer.stop();
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.running = false;
		Collection<C> listenerContainersToStop = getListenerContainers();
		if (listenerContainersToStop.size() > 0) {
			AggregatingCallback aggregatingCallback = new AggregatingCallback(listenerContainersToStop.size(),
					callback);
			for (C listenerContainer : listenerContainersToStop) {
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

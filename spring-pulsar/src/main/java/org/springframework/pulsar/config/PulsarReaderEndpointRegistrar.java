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

package org.springframework.pulsar.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Helper bean for registering {@link PulsarReaderEndpoint} with a
 * {@link GenericReaderEndpointRegistry}.
 *
 * @author Soby Chacko
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PulsarReaderEndpointRegistrar implements BeanFactoryAware, InitializingBean {

	private final Class<? extends ReaderContainerFactory> type;

	private final List<PulsarReaderEndpointDescriptor> endpointDescriptors = new ArrayList<>();

	private final ReentrantLock endpointDescriptorsLock = new ReentrantLock();

	private GenericReaderEndpointRegistry endpointRegistry;

	private ReaderContainerFactory<?, ?> containerFactory;

	private String containerFactoryBeanName;

	private BeanFactory beanFactory;

	private boolean startImmediately;

	public PulsarReaderEndpointRegistrar(Class<? extends ReaderContainerFactory> type) {
		this.type = type;
	}

	public void setEndpointRegistry(GenericReaderEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	@Nullable
	public GenericReaderEndpointRegistry getEndpointRegistry() {
		return this.endpointRegistry;
	}

	public void setContainerFactory(ReaderContainerFactory<?, ?> containerFactory) {
		this.containerFactory = containerFactory;
	}

	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	@Override
	public void afterPropertiesSet() {
		registerAllEndpoints();
	}

	protected void registerAllEndpoints() {
		this.endpointDescriptorsLock.lock();
		try {
			for (PulsarReaderEndpointDescriptor descriptor : this.endpointDescriptors) {
				ReaderContainerFactory<?, ?> factory = resolveContainerFactory(descriptor);
				this.endpointRegistry.registerReaderContainer(descriptor.endpoint, factory);
			}
			this.startImmediately = true; // trigger immediate startup
		}
		finally {
			this.endpointDescriptorsLock.unlock();
		}
	}

	private ReaderContainerFactory<?, ?> resolveContainerFactory(PulsarReaderEndpointDescriptor descriptor) {
		if (descriptor.containerFactory != null) {
			return descriptor.containerFactory;
		}
		else if (this.containerFactory != null) {
			return this.containerFactory;
		}
		else if (this.containerFactoryBeanName != null) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
			this.containerFactory = this.beanFactory.getBean(this.containerFactoryBeanName, this.type);
			return this.containerFactory; // Consider changing this if live change of the
			// factory is required
		}
		else {
			throw new IllegalStateException("Could not resolve the " + ListenerContainerFactory.class.getSimpleName()
					+ " to use for [" + descriptor.endpoint + "] no factory was given and no default is set.");
		}
	}

	public void registerEndpoint(PulsarReaderEndpoint endpoint, @Nullable ReaderContainerFactory<?, ?> factory) {
		Assert.notNull(endpoint, "Endpoint must be set");
		Assert.hasText(endpoint.getId(), "Endpoint id must be set");
		// Factory may be null, we defer the resolution right before actually creating the
		// container
		PulsarReaderEndpointDescriptor descriptor = new PulsarReaderEndpointDescriptor(endpoint, factory);
		this.endpointDescriptorsLock.lock();
		try {
			if (this.startImmediately) { // Register and start immediately
				this.endpointRegistry.registerReaderContainer(descriptor.endpoint, resolveContainerFactory(descriptor),
						true);
			}
			else {
				this.endpointDescriptors.add(descriptor);
			}
		}
		finally {
			this.endpointDescriptorsLock.unlock();
		}
	}

	private static final class PulsarReaderEndpointDescriptor {

		private final PulsarReaderEndpoint endpoint;

		private final ReaderContainerFactory<?, ?> containerFactory;

		private PulsarReaderEndpointDescriptor(PulsarReaderEndpoint endpoint,
				@Nullable ReaderContainerFactory<?, ?> containerFactory) {

			this.endpoint = endpoint;
			this.containerFactory = containerFactory;
		}

	}

}

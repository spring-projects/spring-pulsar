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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;

/**
 * Helper bean for registering {@link ListenerEndpoint} with a
 * {@link ListenerEndpointRegistry}.
 *
 * @author Soby Chacko
 * @author Christophe Bornet
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PulsarListenerEndpointRegistrar implements BeanFactoryAware, InitializingBean {

	private final Class<? extends ListenerContainerFactory> type;

	private final List<PulsarListenerEndpointDescriptor> endpointDescriptors = new ArrayList<>();

	private ListenerEndpointRegistry endpointRegistry;

	private List<HandlerMethodArgumentResolver> customMethodArgumentResolvers = new ArrayList<>();

	private Validator validator;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private ListenerContainerFactory<?, ?> containerFactory;

	private String containerFactoryBeanName;

	private BeanFactory beanFactory;

	private boolean startImmediately;

	public PulsarListenerEndpointRegistrar(Class<? extends ListenerContainerFactory> type) {
		this.type = type;
	}

	public void setEndpointRegistry(ListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	@Nullable
	public ListenerEndpointRegistry getEndpointRegistry() {
		return this.endpointRegistry;
	}

	public List<HandlerMethodArgumentResolver> getCustomMethodArgumentResolvers() {
		return Collections.unmodifiableList(this.customMethodArgumentResolvers);
	}

	public void setCustomMethodArgumentResolvers(HandlerMethodArgumentResolver... methodArgumentResolvers) {
		this.customMethodArgumentResolvers = Arrays.asList(methodArgumentResolvers);
	}

	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory PulsarHandlerMethodFactory) {
		Assert.isNull(this.validator, "A validator cannot be provided with a custom message handler factory");
		this.messageHandlerMethodFactory = PulsarHandlerMethodFactory;
	}

	@Nullable
	public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		return this.messageHandlerMethodFactory;
	}

	public void setContainerFactory(ListenerContainerFactory<?, ?> containerFactory) {
		this.containerFactory = containerFactory;
	}

	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	@Nullable
	public Validator getValidator() {
		return this.validator;
	}

	public void setValidator(Validator validator) {
		Assert.isNull(this.messageHandlerMethodFactory,
				"A validator cannot be provided with a custom message handler factory");
		this.validator = validator;
	}

	@Override
	public void afterPropertiesSet() {
		registerAllEndpoints();
	}

	protected void registerAllEndpoints() {
		synchronized (this.endpointDescriptors) {
			for (PulsarListenerEndpointDescriptor descriptor : this.endpointDescriptors) {
				ListenerContainerFactory<?, ?> factory = resolveContainerFactory(descriptor);
				this.endpointRegistry.registerListenerContainer(descriptor.endpoint, factory);
			}
			this.startImmediately = true; // trigger immediate startup
		}
	}

	private ListenerContainerFactory<?, ?> resolveContainerFactory(PulsarListenerEndpointDescriptor descriptor) {
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

	public void registerEndpoint(ListenerEndpoint endpoint, @Nullable ListenerContainerFactory<?, ?> factory) {
		Assert.notNull(endpoint, "Endpoint must be set");
		Assert.hasText(endpoint.getSubscriptionName(), "Endpoint id must be set");
		// Factory may be null, we defer the resolution right before actually creating the
		// container
		PulsarListenerEndpointDescriptor descriptor = new PulsarListenerEndpointDescriptor(endpoint, factory);
		synchronized (this.endpointDescriptors) {
			if (this.startImmediately) { // Register and start immediately
				this.endpointRegistry.registerListenerContainer(descriptor.endpoint,
						resolveContainerFactory(descriptor), true);
			}
			else {
				this.endpointDescriptors.add(descriptor);
			}
		}
	}

	private static final class PulsarListenerEndpointDescriptor {

		private final ListenerEndpoint endpoint;

		private final ListenerContainerFactory<?, ?> containerFactory;

		private PulsarListenerEndpointDescriptor(ListenerEndpoint endpoint,
				@Nullable ListenerContainerFactory<?, ?> containerFactory) {

			this.endpoint = endpoint;
			this.containerFactory = containerFactory;
		}

	}

}

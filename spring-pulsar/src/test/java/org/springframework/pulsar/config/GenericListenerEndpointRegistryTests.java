/*
 * Copyright 2026-present the original author or authors.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.pulsar.listener.ConcurrentPulsarMessageListenerContainer;

/**
 * Unit tests for {@link GenericListenerEndpointRegistry}.
 *
 * @author Hyun Lee
 */
class GenericListenerEndpointRegistryTests {

	private final PulsarListenerEndpointRegistry registry = new PulsarListenerEndpointRegistry();

	@Test
	void registerWithStartImmediatelyStartsAutoStartupContainer() {
		var container = registerContainer(true, true);
		verify(container).start();
	}

	@Test
	void registerWithStartImmediatelyStartsContainerOnceContextIsRefreshed() {
		var applicationContext = mock(ConfigurableApplicationContext.class);
		this.registry.setApplicationContext(applicationContext);
		this.registry.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		var container = registerContainer(false, true);
		verify(container).start();
	}

	@Test
	void registerWithStartImmediatelyDoesNotStartContainerWithoutAutoStartupBeforeContextIsRefreshed() {
		var container = registerContainer(false, true);
		verify(container, never()).start();
	}

	@Test
	void registerWithoutStartImmediatelyDoesNotStartContainer() {
		var container = registerContainer(true, false);
		verify(container, never()).start();
	}

	private ConcurrentPulsarMessageListenerContainer<?> registerContainer(boolean autoStartup,
			boolean startImmediately) {
		var endpoint = mock(PulsarListenerEndpoint.class);
		when(endpoint.getId()).thenReturn("foo");
		ConcurrentPulsarMessageListenerContainer<?> container = mock();
		when(container.isAutoStartup()).thenReturn(autoStartup);
		var factory = mock(PulsarListenerContainerFactory.class);
		when(factory.createRegisteredContainer(endpoint)).thenReturn(container);
		this.registry.registerListenerContainer(endpoint, factory, startImmediately);
		return container;
	}

}

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
import org.springframework.pulsar.reader.DefaultPulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarMessageReaderContainer;

/**
 * Unit tests for {@link GenericReaderEndpointRegistry}.
 *
 * @author Soby Chacko
 */
class GenericReaderEndpointRegistryTests {

	private final PulsarReaderEndpointRegistry registry = new PulsarReaderEndpointRegistry();

	@Test
	void registerWithStartImmediatelyStartsAutoStartupContainer() {
		var container = registerContainer(true, true);
		verify(container).start();
	}

	@Test
	void registerWithStartImmediatelyDoesNotStartContainerWithoutAutoStartupAfterContextIsRefreshed() {
		var applicationContext = mock(ConfigurableApplicationContext.class);
		this.registry.setApplicationContext(applicationContext);
		this.registry.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		var container = registerContainer(false, true);
		verify(container, never()).start();
	}

	@Test
	void registerWithoutStartImmediatelyDoesNotStartContainer() {
		var container = registerContainer(true, false);
		verify(container, never()).start();
	}

	private DefaultPulsarMessageReaderContainer<?> registerContainer(boolean autoStartup, boolean startImmediately) {
		PulsarReaderEndpoint<PulsarMessageReaderContainer> endpoint = mock();
		when(endpoint.getId()).thenReturn("foo");
		DefaultPulsarMessageReaderContainer<?> container = mock();
		when(container.isAutoStartup()).thenReturn(autoStartup);
		PulsarReaderContainerFactory factory = mock();
		when(factory.createRegisteredContainer(endpoint)).thenReturn(container);
		this.registry.registerReaderContainer(endpoint, factory, startImmediately);
		return container;
	}

}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.springframework.pulsar.listener.ConcurrentPulsarMessageListenerContainer;

/**
 * Unit tests for {@link PulsarListenerEndpointRegistry}.
 *
 * @author Hyun Lee
 */
class PulsarListenerEndpointRegistryTests {

	private final PulsarListenerEndpointRegistry registry = new PulsarListenerEndpointRegistry();

	@Test
	void unregisterListenerContainerRemovesContainerWithoutStoppingIt() {
		var container = registerContainer("foo");
		assertThat(this.registry.unregisterListenerContainer("foo")).isSameAs(container);
		assertThat(this.registry.getListenerContainer("foo")).isNull();
		assertThat(this.registry.getListenerContainerIds()).isEmpty();
		verify(container, never()).stop();
		verify(container, never()).destroy();
	}

	@Test
	void unregisterListenerContainerReturnsNullForUnknownId() {
		assertThat(this.registry.unregisterListenerContainer("foo")).isNull();
	}

	@Test
	void endpointIdCanBeReusedAfterUnregister() {
		registerContainer("foo");
		this.registry.unregisterListenerContainer("foo");
		var newContainer = registerContainer("foo");
		assertThat(this.registry.getListenerContainer("foo")).isSameAs(newContainer);
	}

	private ConcurrentPulsarMessageListenerContainer<?> registerContainer(String id) {
		var endpoint = mock(PulsarListenerEndpoint.class);
		when(endpoint.getId()).thenReturn(id);
		ConcurrentPulsarMessageListenerContainer<?> container = mock();
		var factory = mock(PulsarListenerContainerFactory.class);
		when(factory.createRegisteredContainer(endpoint)).thenReturn(container);
		this.registry.registerListenerContainer(endpoint, factory);
		return container;
	}

}

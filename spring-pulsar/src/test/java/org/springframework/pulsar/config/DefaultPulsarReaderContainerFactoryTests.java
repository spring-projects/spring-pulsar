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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.reader.PulsarReaderContainerProperties;

/**
 * Unit tests for {@link DefaultPulsarReaderContainerFactory}.
 */
class DefaultPulsarReaderContainerFactoryTests {

	@SuppressWarnings({ "removal", "unchecked" })
	@Test
	void deprecatedCreateReaderContainerWithEndpointCallsReplacementApi() {
		var containerFactory = spy(new DefaultPulsarReaderContainerFactory<>(mock(PulsarReaderFactory.class),
				new PulsarReaderContainerProperties()));
		var endpoint = mock(PulsarReaderEndpoint.class);
		var createdContainer = containerFactory.createReaderContainer(endpoint);
		assertThat(createdContainer).isNotNull();
		verify(containerFactory).createRegisteredContainer(endpoint);
	}

	@SuppressWarnings({ "removal", "unchecked" })
	@Test
	void deprecatedCreateReaderContainerWithTopicsCallsReplacementApi() {
		var containerFactory = spy(new DefaultPulsarReaderContainerFactory<>(mock(PulsarReaderFactory.class),
				new PulsarReaderContainerProperties()));
		var createdContainer = containerFactory.createReaderContainer("my-topic");
		// reader does not implement this API - still ensure the replacement API is called
		assertThat(createdContainer).isNull();
		verify(containerFactory).createContainer("my-topic");
	}

}

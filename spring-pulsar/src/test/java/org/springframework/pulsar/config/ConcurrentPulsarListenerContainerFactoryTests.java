/*
 * Copyright 2025-2025 the original author or authors.
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
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.PulsarContainerProperties;

/**
 * Unit tests for {@link ConcurrentPulsarListenerContainerFactory}.
 *
 * @author Daniel Szabo
 * @author Chris Bono
 */
class ConcurrentPulsarListenerContainerFactoryTests {

	@Nested
	class ConsumerTaskExecutorFrom {

		@Test
		@SuppressWarnings("unchecked")
		void factoryPropsUsedWhenSpecified() {
			var factoryProps = new PulsarContainerProperties();
			AsyncTaskExecutor executor = mock();
			factoryProps.setConsumerTaskExecutor(executor);
			var containerFactory = new ConcurrentPulsarListenerContainerFactory<String>(
					mock(PulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(PulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);

			var container = containerFactory.createContainerInstance(endpoint);
			assertThat(container.getContainerProperties())
				.extracting(PulsarContainerProperties::getConsumerTaskExecutor)
				.isSameAs(executor);
		}

	}

}

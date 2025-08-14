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

package org.springframework.pulsar.reactive.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;

/**
 * Unit tests for {@link DefaultReactivePulsarListenerContainerFactory}.
 */
class DefaultReactivePulsarListenerContainerFactoryTests {

	@SuppressWarnings("unchecked")
	@Nested
	class SubscriptionTypeFrom {

		@Test
		void factoryPropsUsedWhenNotSetOnEndpoint() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			factoryProps.setSubscriptionType(SubscriptionType.Shared);
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);
			var createdContainer = containerFactory.createRegisteredContainer(endpoint);
			assertThat(createdContainer.getContainerProperties().getSubscriptionType())
				.isEqualTo(SubscriptionType.Shared);
		}

		@Test
		void endpointTakesPrecedenceOverFactoryProps() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			factoryProps.setSubscriptionType(SubscriptionType.Shared);
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);
			when(endpoint.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
			var createdContainer = containerFactory.createRegisteredContainer(endpoint);
			assertThat(createdContainer.getContainerProperties().getSubscriptionType())
				.isEqualTo(SubscriptionType.Failover);
		}

		@Test
		void defaultUsedWhenNotSetOnEndpointNorFactoryProps() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);
			var createdContainer = containerFactory.createRegisteredContainer(endpoint);
			assertThat(createdContainer.getContainerProperties().getSubscriptionType())
				.isEqualTo(SubscriptionType.Exclusive);

		}

	}

	@SuppressWarnings("unchecked")
	@Nested
	class SubscriptionNameFrom {

		@Test
		void factoryPropsUsedWhenNotSetOnEndpoint() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			factoryProps.setSubscriptionName("my-factory-subscription");
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);
			var createdContainer = containerFactory.createRegisteredContainer(endpoint);
			assertThat(createdContainer.getContainerProperties().getSubscriptionName())
				.isEqualTo("my-factory-subscription");
		}

		@Test
		void endpointTakesPrecedenceOverFactoryProps() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			factoryProps.setSubscriptionName("my-factory-subscription");
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);
			when(endpoint.getSubscriptionName()).thenReturn("my-endpoint-subscription");
			var createdContainer = containerFactory.createRegisteredContainer(endpoint);
			assertThat(createdContainer.getContainerProperties().getSubscriptionName())
				.isEqualTo("my-endpoint-subscription");
		}

		@Test
		void defaultUsedWhenNotSetOnEndpointNorFactoryProps() {
			var factoryProps = new ReactivePulsarContainerProperties<String>();
			var containerFactory = new DefaultReactivePulsarListenerContainerFactory<String>(
					mock(ReactivePulsarConsumerFactory.class), factoryProps);
			var endpoint = mock(ReactivePulsarListenerEndpoint.class);
			when(endpoint.getConcurrency()).thenReturn(1);

			var container1 = containerFactory.createRegisteredContainer(endpoint);
			assertThat(container1.getContainerProperties().getSubscriptionName())
				.startsWith("org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#");
			var container2 = containerFactory.createRegisteredContainer(endpoint);
			assertThat(container2.getContainerProperties().getSubscriptionName())
				.startsWith("org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#");
			assertThat(container1.getContainerProperties().getSubscriptionName())
				.isNotEqualTo(container2.getContainerProperties().getSubscriptionName());
		}

	}

}

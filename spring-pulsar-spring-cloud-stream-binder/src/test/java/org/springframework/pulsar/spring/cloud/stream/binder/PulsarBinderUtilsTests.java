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

package org.springframework.pulsar.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;

/**
 * Unit tests for {@link PulsarBinderUtils}.
 *
 * @author Soby Chacko
 */
public class PulsarBinderUtilsTests {

	@Test
	void subscriptionNameIsNotNullWhenProvidedAsProperty() {
		ConsumerDestination consumerDestination = mock(ConsumerDestination.class);
		PulsarConsumerProperties pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
		when(pulsarConsumerProperties.getSubscriptionName()).thenReturn("my-subscription");
		String subscriptionName = PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination);
		assertThat(subscriptionName).isEqualTo("my-subscription");
	}

	@Test
	void subscriptionNameIsNotNullWhenPropertyIsMissing() {
		ConsumerDestination consumerDestination = mock(ConsumerDestination.class);
		PulsarConsumerProperties pulsarConsumerProperties = mock(PulsarConsumerProperties.class);
		when(pulsarConsumerProperties.getSubscriptionName()).thenReturn(null);
		when(consumerDestination.getName()).thenReturn("my-topic");
		String subscriptionName = PulsarBinderUtils.subscriptionName(pulsarConsumerProperties, consumerDestination);
		assertThat(subscriptionName).startsWith("my-topic" + PulsarBinderUtils.SUBSCRIPTION_NAME_SEPARATOR
				+ PulsarBinderUtils.ANON_SUBSCRIPTION + PulsarBinderUtils.SUBSCRIPTION_NAME_SEPARATOR);
	}

}

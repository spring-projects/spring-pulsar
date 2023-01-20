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

import java.util.UUID;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;

/**
 * Utility methods for the binder.
 *
 * @author Soby Chacko
 */
public final class PulsarBinderUtils {

	static final String ANON_SUBSCRIPTION = "anon-subscription";

	static final char SUBSCRIPTION_NAME_SEPARATOR = '-';

	private PulsarBinderUtils() {

	}

	static String subscriptionName(PulsarConsumerProperties pulsarConsumerProperties,
			ConsumerDestination consumerDestination) {
		String subscriptionName = pulsarConsumerProperties.getSubscriptionName();
		if (subscriptionName == null) {
			// if subscription name is not provided, each time the app starts, it will be
			// an anonymous subscription
			subscriptionName = consumerDestination.getName() + SUBSCRIPTION_NAME_SEPARATOR + ANON_SUBSCRIPTION
					+ SUBSCRIPTION_NAME_SEPARATOR + UUID.randomUUID();
		}
		return subscriptionName;
	}

}

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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;
import org.springframework.util.StringUtils;

/**
 * Binder utility methods.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
final class PulsarBinderUtils {

	private static final LogAccessor LOGGER = new LogAccessor(PulsarBinderUtils.class);

	private static final String SUBSCRIPTION_NAME_FORMAT_STR = "%s-anon-subscription-%s";

	private PulsarBinderUtils() {
	}

	/**
	 * Gets the subscription name to use for the binder.
	 * @param consumerProps the pulsar consumer props
	 * @param consumerDestination the destination being subscribed to
	 * @return the subscription name from the consumer properties or a generated name in
	 * the format {@link #SUBSCRIPTION_NAME_FORMAT_STR} when the name is not set on the
	 * consumer properties
	 */
	static String subscriptionName(PulsarConsumerProperties consumerProps, ConsumerDestination consumerDestination) {
		if (StringUtils.hasText(consumerProps.getSubscriptionName())) {
			return consumerProps.getSubscriptionName();
		}
		return SUBSCRIPTION_NAME_FORMAT_STR.formatted(consumerDestination.getName(), UUID.randomUUID());
	}

	/**
	 * Merges properties defined at the binder and binding level (binding properties
	 * override binder properties).
	 * <p>
	 * <b>NOTE:</b> Properties whose value is not different from the default value in the
	 * {@code baseProps} are not included in the merged result.
	 * @param baseProps the map of base level properties (eg. 'spring.pulsar.consumer.*')
	 * @param binderProps the map of binder level properties (eg.
	 * 'spring.cloud.stream.pulsar.binder.consumer.*')
	 * @param bindingProps the map of binding level properties (eg.
	 * 'spring.cloud.stream.pulsar.bindings.myBinding-in-0.consumer.*')
	 * @return map of merged binder and binding properties including only properties whose
	 * value has changed from the same property in the base properties
	 */
	static Map<String, Object> mergePropertiesWithPrecedence(Map<String, Object> baseProps,
			Map<String, Object> binderProps, Map<String, Object> bindingProps) {
		Objects.requireNonNull(baseProps, "baseProps must be specified");
		Objects.requireNonNull(binderProps, "binderProps must be specified");
		Objects.requireNonNull(bindingProps, "bindingProps must be specified");

		Map<String, Object> newOrModifiedBinderProps = extractNewOrModifiedProperties(binderProps, baseProps);
		LOGGER.trace(() -> "New or modified binder props: %s".formatted(newOrModifiedBinderProps));

		Map<String, Object> newOrModifiedBindingProps = extractNewOrModifiedProperties(bindingProps, baseProps);
		LOGGER.trace(() -> "New or modified binding props: %s".formatted(newOrModifiedBindingProps));

		Map<String, Object> mergedProps = new HashMap<>(newOrModifiedBinderProps);
		mergedProps.putAll(newOrModifiedBindingProps);
		LOGGER.trace(() -> "Final merged props: %s".formatted(mergedProps));

		return mergedProps;
	}

	private static Map<String, Object> extractNewOrModifiedProperties(Map<String, Object> candidateProps,
			Map<String, Object> baseProps) {
		Map<String, Object> newOrModifiedProps = new HashMap<>();
		candidateProps.forEach((propName, propValue) -> {
			if (!baseProps.containsKey(propName) || (!Objects.equals(propValue, baseProps.get(propName)))) {
				newOrModifiedProps.put(propName, propValue);
			}
		});
		return newOrModifiedProps;
	}

}

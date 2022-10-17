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

package org.springframework.pulsar.core.reactive;

import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;

import org.springframework.util.StringUtils;

/**
 * Common utilities used by reactive sender components.
 *
 * @author Christophe Bornet
 */
final class ReactiveMessageSenderUtils {

	private ReactiveMessageSenderUtils() {
	}

	static <T> String resolveTopicName(String userSpecifiedTopic,
			ReactivePulsarSenderFactory<T> reactiveMessageSenderFactory) {
		ReactiveMessageSenderSpec reactiveMessageSenderSpec = reactiveMessageSenderFactory
				.getReactiveMessageSenderSpec();
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return userSpecifiedTopic;
		}
		else if (reactiveMessageSenderSpec != null && reactiveMessageSenderSpec.getTopicName() != null) {
			return reactiveMessageSenderSpec.getTopicName();
		}
		else {
			throw new IllegalArgumentException("Topic must be specified when no default topic is configured");
		}
	}

}

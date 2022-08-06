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

package org.springframework.pulsar.core;

import java.util.Optional;

import org.apache.pulsar.client.api.Producer;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.StringUtils;

/**
 * Common utilities used by producer components.
 *
 * @author Chris Bono
 */
final class ProducerUtils {

	private ProducerUtils() {
	}

	static <T> String formatProducer(Producer<T> producer) {
		return String.format("(%s:%s)", producer.getProducerName(), producer.getTopic());
	}

	static <T> String resolveTopicName(String userSpecifiedTopic, PulsarProducerFactory<T> producerFactory) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return userSpecifiedTopic;
		}
		return Optional.ofNullable(producerFactory.getProducerConfig().get("topicName")).map(Object::toString)
				.orElseThrow(() -> new IllegalArgumentException(
						"Topic must be specified when no default topic is configured"));
	}

	static <T> void closeProducerAsync(Producer<T> producer, LogAccessor logger) {
		producer.closeAsync().exceptionally(e -> {
			logger.warn(e, () -> String.format("Failed to close producer %s", ProducerUtils.formatProducer(producer)));
			return null;
		});
	}

}

/*
 * Copyright 2022-2023 the original author or authors.
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

import org.apache.pulsar.client.api.Producer;

import org.springframework.core.log.LogAccessor;

/**
 * Common utilities used by producer components.
 *
 * @author Chris Bono
 */
final class ProducerUtils {

	private ProducerUtils() {
	}

	static <T> String formatProducer(Producer<T> producer) {
		return "(%s:%s)".formatted(producer.getProducerName(), producer.getTopic());
	}

	static <T> void closeProducerAsync(Producer<T> producer, LogAccessor logger) {
		producer.closeAsync().exceptionally(e -> {
			logger.warn(e, () -> "Failed to close producer %s".formatted(ProducerUtils.formatProducer(producer)));
			return null;
		});
	}

}

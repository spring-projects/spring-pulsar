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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

	static <T> CompletableFuture<Void> closeProducerAsync(Producer<T> producer, LogAccessor logger) {
		return producer.closeAsync().exceptionally(e -> {
			logger.warn(e, () -> "Failed to close producer %s".formatted(ProducerUtils.formatProducer(producer)));
			return null;
		});
	}

	static <T> void closeProducer(Producer<T> producer, LogAccessor logger, Duration maxWaitTime) {
		try {
			producer.closeAsync().get(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (Exception e) {
			logger.warn(e, () -> "Failed to close producer %s".formatted(ProducerUtils.formatProducer(producer)));
		}
	}

}

/*
 * Copyright 2023-2024 the original author or authors.
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
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.annotation.PulsarMessage;
import org.springframework.util.Assert;

/**
 * A registry that holds the {@link PulsarMessage @PulsarMessage} annotations and each
 * associated class that is marked with the annotation.
 * <p>
 * The annotations are looked up on-demand and the result is cached.
 * <p>
 * Once the cache reaches a {@link #maxNumberOfAnnotationsCached certain size} (default of
 * {@link #DEFAULT_MAX_CACHE_SIZE}) it is cleared and the annotations will be looked up
 * again the next time they are requested.
 *
 * @author Chris Bono
 */
class PulsarMessageAnnotationRegistry {

	private static final int DEFAULT_MAX_CACHE_SIZE = 1000;

	private final int maxNumberOfAnnotationsCached;

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private ConcurrentHashMap<Class<?>, Optional<PulsarMessage>> annotationsByClass = new ConcurrentHashMap<>();

	PulsarMessageAnnotationRegistry() {
		this(DEFAULT_MAX_CACHE_SIZE);
	}

	PulsarMessageAnnotationRegistry(int maxNumberOfAnnotationsCached) {
		Assert.state(maxNumberOfAnnotationsCached > 0, "maxNumberOfAnnotationsCached must be > 0");
		this.maxNumberOfAnnotationsCached = maxNumberOfAnnotationsCached;
	}

	/**
	 * Gets the {@link PulsarMessage @PulsarMessage} on the specified class or empty if
	 * the class is not marked with the annotation.
	 * @param targetClass the class to check for the annotation
	 * @return an optional containing the annotation or empty if the class is not marked
	 * with the annotation.
	 */
	Optional<PulsarMessage> getAnnotationFor(Class<?> targetClass) {
		var annotation = this.annotationsByClass.computeIfAbsent(targetClass, this::findAnnotationOn);
		if (this.annotationsByClass.size() > this.maxNumberOfAnnotationsCached) {
			this.logger
				.info(() -> "Clearing cache - max entries exceeded (%d)".formatted(this.maxNumberOfAnnotationsCached));
			this.annotationsByClass = new ConcurrentHashMap<>();
		}
		return annotation;
	}

	// VisibleForTesting
	protected Optional<PulsarMessage> findAnnotationOn(Class<?> targetClass) {
		this.logger.debug(() -> "Looking for @PulsarMessage on " + targetClass);
		PulsarMessage annotation = AnnotationUtils.findAnnotation(targetClass, PulsarMessage.class);
		return Optional.ofNullable(annotation);
	}

}

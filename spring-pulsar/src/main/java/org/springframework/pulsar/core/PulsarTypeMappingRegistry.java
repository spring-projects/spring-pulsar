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
import org.springframework.pulsar.annotation.PulsarTypeMapping;
import org.springframework.util.Assert;

/**
 * A registry that holds the {@link PulsarTypeMapping @PulsarTypeMapping} annotations and
 * each associated class that is marked with the annotation.
 * <p>
 * The annotations are looked up on-demand and the result is cached.
 * <p>
 * Once the cache reaches a {@link #maxNumberOfMappingsCached certain size} (default of
 * {@link #DEFAULT_MAX_CACHE_SIZE}) it is cleared and the annotations will be looked up
 * again the next time they are requested.
 *
 * @author Chris Bono
 */
class PulsarTypeMappingRegistry {

	private static final int DEFAULT_MAX_CACHE_SIZE = 1000;

	private final int maxNumberOfMappingsCached;

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private ConcurrentHashMap<Class<?>, Optional<PulsarTypeMapping>> typeMappingsByClass = new ConcurrentHashMap<>();

	PulsarTypeMappingRegistry() {
		this(DEFAULT_MAX_CACHE_SIZE);
	}

	PulsarTypeMappingRegistry(int maxNumberOfMappingsCached) {
		Assert.state(maxNumberOfMappingsCached > 0, "maxNumberOfMappingsCached must be > 0");
		this.maxNumberOfMappingsCached = maxNumberOfMappingsCached;
	}

	/**
	 * Gets the {@link PulsarTypeMapping @PulsarTypeMapping} on the specified class or
	 * empty if the class is not marked with the annotation.
	 * @param targetClass the class to check for the annotation
	 * @return an optional containing the annotation or empty if the class is not marked
	 * with the annotation.
	 */
	Optional<PulsarTypeMapping> getTypeMappingFor(Class<?> targetClass) {
		var optionalTypeMapping = this.typeMappingsByClass.computeIfAbsent(targetClass, this::findTypeMappingOn);
		if (this.typeMappingsByClass.size() > this.maxNumberOfMappingsCached) {
			this.logger
				.info(() -> "Clearing cache - max entries exceeded (%d)".formatted(this.maxNumberOfMappingsCached));
			this.typeMappingsByClass = new ConcurrentHashMap<>();
		}
		return optionalTypeMapping;
	}

	// VisibleForTesting
	protected Optional<PulsarTypeMapping> findTypeMappingOn(Class<?> targetClass) {
		this.logger.debug(() -> "Looking for @PulsarTypeMapping on " + targetClass);
		PulsarTypeMapping annotation = AnnotationUtils.findAnnotation(targetClass, PulsarTypeMapping.class);
		return Optional.ofNullable(annotation);
	}

}

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

package org.springframework.pulsar.core;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * Cache provider implementation backed by a {@code Caffeine} cache.
 *
 * @param <K> the type of cache key
 * @param <V> the type of cache entries
 * @author Chris Bono
 */
public class CaffeineCacheProvider<K, V> implements CacheProvider<K, V> {

	private final Cache<K, V> cache;

	public CaffeineCacheProvider(Cache<K, V> cache) {
		this.cache = cache;
	}

	@Override
	public V getOrCreateIfAbsent(K cacheKey, Function<K, V> createEntryFunction) {
		return this.cache.get(cacheKey, createEntryFunction);
	}

	@Override
	public Map<K, V> asMap() {
		return this.cache.asMap();
	}

	@Override
	public void invalidateAll(BiConsumer<K, V> onInvalidateEntry) {
		this.cache.asMap().forEach((cacheKey, cacheEntry) -> {
			this.cache.invalidate(cacheKey);
			onInvalidateEntry.accept(cacheKey, cacheEntry);
		});
	}

}

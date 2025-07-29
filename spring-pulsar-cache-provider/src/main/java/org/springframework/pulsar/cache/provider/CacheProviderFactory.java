/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.pulsar.cache.provider;

import java.time.Duration;
import java.util.ServiceLoader;

/**
 * Interface to create instances of {@link CacheProvider}.
 *
 * @param <K> the type of cache key
 * @param <V> the type of cache entries
 * @author Chris Bono
 */
public interface CacheProviderFactory<K, V> {

	/**
	 * Create a cache provider instance with the specified options.
	 * @param cacheExpireAfterAccess time period to expire unused entries in the cache
	 * @param cacheMaximumSize maximum size of cache (entries)
	 * @param cacheInitialCapacity the initial size of cache
	 * @param evictionListener listener called when an entry is evicted from the cache
	 * @return cache provider instance
	 */
	CacheProvider<K, V> create(Duration cacheExpireAfterAccess, Long cacheMaximumSize, Integer cacheInitialCapacity,
			EvictionListener<K, V> evictionListener);

	/**
	 * Uses the Java ServiceLoader API to find the first available cache provider factory.
	 * @param <K> the type of cache key
	 * @param <V> the type of cache entries
	 * @return the cache provider factory service
	 * @throws IllegalStateException if no factory was found
	 */
	@SuppressWarnings("unchecked")
	static <K, V> CacheProviderFactory<K, V> load() {
		return ServiceLoader.load(CacheProviderFactory.class)
			.findFirst()
			.orElseThrow(() -> new IllegalStateException("No ProducerCacheFactory available"));
	}

	/**
	 * Interface for a cache eviction listener.
	 *
	 * @param <K> the type of cache key
	 * @param <V> the type of cache entries
	 */
	interface EvictionListener<K, V> {

		/**
		 * Called when an entry is evicted from the cache.
		 * @param key the cache key
		 * @param value the cached value
		 * @param reason the reason for the eviction
		 */
		void onEviction(K key, V value, String reason);

	}

}

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

package org.springframework.pulsar.cache.provider;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Defines the contract for a cache provider.
 *
 * @param <K> the type of cache key
 * @param <V> the type of cache entries
 * @author Chris Bono
 */
public interface CacheProvider<K, V> {

	/**
	 * Returns the value associated with the {@code key} in the cache.
	 * <p>
	 * If the key is not already associated with an entry in the cache, the
	 * {@code createEntryFunction} is used to compute the value to cache and return.
	 * @param key the cache key
	 * @param createEntryFunction the function to compute a value
	 * @return the current (existing or computed) value associated with the specified key,
	 * or null if the computed value is null
	 */
	V getOrCreateIfAbsent(K key, Function<K, V> createEntryFunction);

	/**
	 * Returns a view of the entries stored in the cache as a thread-safe map.
	 * Modifications made to the map directly affect the cache.
	 * @return a thread-safe view of the cache supporting all optional {@link Map}
	 * operations
	 */
	Map<K, V> asMap();

	/**
	 * Discards all entries in the cache and calls the {@code onInvalidateEntry} callback
	 * (if provided) for each entry.
	 * @param onInvalidateEntry callback invoked for each invalidated entry
	 */
	void invalidateAll(BiConsumer<K, V> onInvalidateEntry);

}

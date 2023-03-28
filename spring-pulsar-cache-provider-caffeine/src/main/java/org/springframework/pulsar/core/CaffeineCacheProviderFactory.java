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

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;

/**
 * Factory to create instances of {@link CaffeineCacheProvider}.
 *
 * @param <K> the type of cache key
 * @param <V> the type of cache entries
 * @author Chris Bono
 */
public class CaffeineCacheProviderFactory<K, V> implements CacheProviderFactory<K, V> {

	@Override
	public CacheProvider<K, V> create(Duration cacheExpireAfterAccess, Long cacheMaximumSize,
			Integer cacheInitialCapacity, EvictionListener<K, V> evictionListener) {
		Cache<K, V> cache = Caffeine.newBuilder().expireAfterAccess(cacheExpireAfterAccess)
				.maximumSize(cacheMaximumSize).initialCapacity(cacheInitialCapacity)
				.scheduler(Scheduler.systemScheduler()).evictionListener((RemovalListener<K, V>) (key, value,
						cause) -> evictionListener.onEviction(key, value, cause.toString()))
				.build();
		return new CaffeineCacheProvider<>(cache);
	}

}

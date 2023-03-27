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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheProviderFactory}.
 *
 * @author Chris Bono
 */
public class CacheProviderFactoryTests {

	@Test
	void loadFactory() {
		assertThat(CacheProviderFactory.load()).isInstanceOf(TestCacheProviderFactory.class);
	}

	public static class TestCacheProviderFactory implements CacheProviderFactory<String, Object> {

		@Override
		public CacheProvider<String, Object> create(Duration cacheExpireAfterAccess, Long cacheMaximumSize,
				Integer cacheInitialCapacity, EvictionListener<String, Object> evictionListener) {
			return null;
		}

	}

}

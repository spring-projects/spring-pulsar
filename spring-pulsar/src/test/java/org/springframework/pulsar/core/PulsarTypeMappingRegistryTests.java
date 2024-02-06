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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.pulsar.annotation.PulsarTypeMapping;

/**
 * Unit tests for {@link PulsarTypeMappingRegistry}.
 *
 * @author Chris Bono
 */
class PulsarTypeMappingRegistryTests {

	@Test
	void typeMappingFoundAndCached() {
		PulsarTypeMappingRegistry registry = spy(new PulsarTypeMappingRegistry(2));
		var typeMapping = registry.getTypeMappingFor(Foo.class);
		assertThat(typeMapping).map(PulsarTypeMapping::topic).hasValue("foo-topic");
		// subsequent calls are cached
		assertThat(registry.getTypeMappingFor(Foo.class)).isSameAs(typeMapping);
		assertThat(registry.getTypeMappingFor(Foo.class)).isSameAs(typeMapping);
		verify(registry, times(1)).findTypeMappingOn(Foo.class);
	}

	@Test
	void typeMappingNotFoundAndCached() {
		PulsarTypeMappingRegistry registry = spy(new PulsarTypeMappingRegistry(2));
		var typeMapping = registry.getTypeMappingFor(Bar.class);
		assertThat(typeMapping).isEmpty();
		// subsequent calls are cached
		assertThat(registry.getTypeMappingFor(Bar.class)).isSameAs(typeMapping);
		assertThat(registry.getTypeMappingFor(Bar.class)).isSameAs(typeMapping);
		verify(registry, times(1)).findTypeMappingOn(Bar.class);
	}

	@Test
	void cacheIsClearedOnceMaxNumberReached() {
		PulsarTypeMappingRegistry registry = spy(new PulsarTypeMappingRegistry(2));
		registry.getTypeMappingFor(Foo.class);
		registry.getTypeMappingFor(Bar.class);
		registry.getTypeMappingFor(Zaa.class);
		// the 3rd request will force a cache clear - subsequent calls will do lookup
		// again
		registry.getTypeMappingFor(Foo.class);
		registry.getTypeMappingFor(Bar.class);
		// now values should be cached again
		registry.getTypeMappingFor(Foo.class);
		registry.getTypeMappingFor(Bar.class);
		verify(registry, times(2)).findTypeMappingOn(Foo.class);
		verify(registry, times(2)).findTypeMappingOn(Bar.class);
	}

	@ParameterizedTest
	@ValueSource(ints = { -1, 0 })
	void maxNumberOfMappingsMustBePositive(int maxNumberOfMappings) {
		assertThatIllegalStateException().isThrownBy(() -> new PulsarTypeMappingRegistry(maxNumberOfMappings))
			.withMessage("maxNumberOfMappingsCached must be > 0");
	}

	@PulsarTypeMapping(topic = "foo-topic")
	record Foo(String value) {
	}

	record Bar(String value) {
	}

	record Zaa(String value) {
	}

}

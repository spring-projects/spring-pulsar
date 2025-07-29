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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.pulsar.annotation.PulsarMessage;

/**
 * Unit tests for {@link PulsarMessageAnnotationRegistry}.
 *
 * @author Chris Bono
 */
class PulsarMessageAnnotationRegistryTests {

	@Test
	void annotationFoundAndCached() {
		PulsarMessageAnnotationRegistry registry = spy(new PulsarMessageAnnotationRegistry(2));
		var annotation = registry.getAnnotationFor(Foo.class);
		assertThat(annotation).map(PulsarMessage::topic).hasValue("foo-topic");
		// subsequent calls are cached
		assertThat(registry.getAnnotationFor(Foo.class)).isSameAs(annotation);
		assertThat(registry.getAnnotationFor(Foo.class)).isSameAs(annotation);
		verify(registry, times(1)).findAnnotationOn(Foo.class);
	}

	@Test
	void annotationNotFoundAndCached() {
		PulsarMessageAnnotationRegistry registry = spy(new PulsarMessageAnnotationRegistry(2));
		var annotation = registry.getAnnotationFor(Bar.class);
		assertThat(annotation).isEmpty();
		// subsequent calls are cached
		assertThat(registry.getAnnotationFor(Bar.class)).isSameAs(annotation);
		assertThat(registry.getAnnotationFor(Bar.class)).isSameAs(annotation);
		verify(registry, times(1)).findAnnotationOn(Bar.class);
	}

	@Test
	void cacheIsClearedOnceMaxNumberReached() {
		PulsarMessageAnnotationRegistry registry = spy(new PulsarMessageAnnotationRegistry(2));
		registry.getAnnotationFor(Foo.class);
		registry.getAnnotationFor(Bar.class);
		registry.getAnnotationFor(Zaa.class);
		// the 3rd request will force a cache clear - subsequent calls will do lookup
		// again
		registry.getAnnotationFor(Foo.class);
		registry.getAnnotationFor(Bar.class);
		// now values should be cached again
		registry.getAnnotationFor(Foo.class);
		registry.getAnnotationFor(Bar.class);
		verify(registry, times(2)).findAnnotationOn(Foo.class);
		verify(registry, times(2)).findAnnotationOn(Bar.class);
	}

	@ParameterizedTest
	@ValueSource(ints = { -1, 0 })
	void maxNumberOfAnnotationsMustBePositive(int maxAnnotations) {
		assertThatIllegalStateException().isThrownBy(() -> new PulsarMessageAnnotationRegistry(maxAnnotations))
			.withMessage("maxNumberOfAnnotationsCached must be > 0");
	}

	@PulsarMessage(topic = "foo-topic")
	record Foo(String value) {
	}

	record Bar(String value) {
	}

	record Zaa(String value) {
	}

}

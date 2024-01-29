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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.function.Consumer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link Resolved}.
 *
 * @author Chris Bono
 */
class ResolvedTests {

	@SuppressWarnings("removal")
	@Test
	void deprecatedGetDelegatesToNewValueMethod() {
		Resolved<String> resolved = Mockito.spy(Resolved.of("hello"));
		resolved.get();
		verify(resolved).value();
	}

	@SuppressWarnings("unchecked")
	static Consumer<String> mockValueAction() {
		return (Consumer<String>) mock(Consumer.class);
	}

	@SuppressWarnings("unchecked")
	static Consumer<RuntimeException> mockErrorAction() {
		return (Consumer<RuntimeException>) mock(Consumer.class);
	}

	@Nested
	class WhenResolutionSucceeds {

		@Test
		void valueDoesReturnValue() {
			var resolved = Resolved.of("smile");
			assertThat(resolved.value()).hasValue("smile");
		}

		@Test
		void orElseThrowDoesReturnValue() {
			var resolved = Resolved.of("smile");
			assertThat(resolved.orElseThrow()).isEqualTo("smile");
		}

		@Test
		void exceptionDoesReturnEmpty() {
			var resolved = Resolved.of("smile");
			assertThat(resolved.exception()).isEmpty();
		}

		@Test
		void ifResolvedDoesCallValueAction() {
			var resolved = Resolved.of("smile");
			var valueAction = mockValueAction();
			resolved.ifResolved(valueAction);
			verify(valueAction).accept("smile");
		}

		@Test
		void ifResolvedOrElseDoesCallValueActionAndIgnoresErrorAction() {
			var resolved = Resolved.of("smile");
			var valueAction = mockValueAction();
			var errorAction = mockErrorAction();
			resolved.ifResolvedOrElse(valueAction, errorAction);
			verify(valueAction).accept("smile");
			verifyNoInteractions(errorAction);
		}

	}

	@Nested
	class WhenResolutionFails {

		@Test
		void valueDoesReturnEmpty() {
			var resolved = Resolved.failed("5150");
			assertThat(resolved.value()).isEmpty();
		}

		@Test
		void orElseThrowDoesThrowSimpleReason() {
			var resolved = Resolved.failed("5150");
			assertThatIllegalArgumentException().isThrownBy(resolved::orElseThrow).withMessage("5150");
		}

		@Test
		void orElseThrowDoesThrowReason() {
			var resolved = Resolved.failed(new IllegalStateException("5150"));
			assertThatIllegalStateException().isThrownBy(resolved::orElseThrow).withMessage("5150");
		}

		@Test
		void orElseThrowDoesThrowReasonWithExtraMessage() {
			var resolved = Resolved.failed(new IllegalStateException("5150"));
			assertThatRuntimeException().isThrownBy(() -> resolved.orElseThrow(() -> "extra message"))
				.withMessage("extra message")
				.withCause(new IllegalStateException("5150"));
		}

		@Test
		void exceptionDoesReturnReason() {
			var resolved = Resolved.failed("5150");
			assertThat(resolved.exception()).hasValueSatisfying(
					(ex) -> assertThat(ex).isInstanceOf(IllegalArgumentException.class).hasMessage("5150"));
		}

		@Test
		void ifResolvedDoesNotCallValueAction() {
			var resolved = Resolved.<String>failed("5150");
			var valueAction = mockValueAction();
			resolved.ifResolved(valueAction);
			verifyNoInteractions(valueAction);
		}

		@Test
		void ifResolvedOrElseDoesIgnoreValueActionAndCallsErrorAction() {
			var resolved = Resolved.<String>failed("5150");
			var valueAction = mockValueAction();
			var errorAction = mockErrorAction();
			resolved.ifResolvedOrElse(valueAction, errorAction);
			verifyNoInteractions(valueAction);
			verify(errorAction).accept(resolved.exception().get());
		}

	}

}

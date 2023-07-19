/*
 * Copyright 2023-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Resolved}.
 *
 * @author Chris Bono
 */
class ResolvedTests {

	@Test
	void success() {
		assertThat(Resolved.of("good").get()).hasValue("good");
		assertThat(Resolved.of("good").orElseThrow()).isEqualTo("good");
	}

	@Test
	void failedWithSimpleReason() {
		var resolved = Resolved.failed("oops");
		assertThatIllegalArgumentException().isThrownBy(resolved::orElseThrow).withMessage("oops");
		assertThat(resolved.get()).isEmpty();
	}

	@Test
	void failedWithReason() {
		var resolved = Resolved.failed(new IllegalStateException("5150"));
		assertThatIllegalStateException().isThrownBy(resolved::orElseThrow).withMessage("5150");
		assertThat(resolved.get()).isEmpty();
	}

	@Test
	void failedWithAdditionalMessage() {
		var resolved = Resolved.failed(new IllegalStateException("5150"));
		assertThatRuntimeException().isThrownBy(() -> resolved.orElseThrow(() -> "extra message"))
			.withMessage("extra message")
			.withCause(new IllegalStateException("5150"));
		assertThat(resolved.get()).isEmpty();
	}

}

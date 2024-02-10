/*
 * Copyright 2012-2024 the original author or authors.
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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResolvedExpression}.
 *
 * @author Jonas Geiregat
 */
public class ResolvedExpressionTest {

	@Test
	public void resolveToStringWithValidString() {
		ResolvedExpression resolved = ResolvedExpression.of("testString");
		assertThat(resolved.resolveToString().value()).isPresent().contains("testString");
	}

	@Test
	public void resolveToStringWithNonString() {
		ResolvedExpression resolved = ResolvedExpression.of(123);
		assertThat(resolved.resolveToString().exception()).isPresent();
		assertThat(resolved.resolveToString().exception().get()).extracting("message")
			.isEqualTo("Resolved to [class java.lang.Integer] for [123]");
	}

	@Test
	public void resolveToStringWithNull() {
		ResolvedExpression resolved = ResolvedExpression.of(null);
		assertThat(resolved.resolveToString().value()).isNotPresent();
	}

	@Test
	public void resolveToStringsWithStringArray() {
		ResolvedExpression resolved = ResolvedExpression.of(new String[] { "test1", "test2" });
		assertThat(resolved.resolveToStrings()).containsExactly("test1", "test2");
	}

	@Test
	public void resolveToStringsWithIterable() {
		ResolvedExpression resolved = ResolvedExpression.of(List.of("test1", "test2"));
		assertThat(resolved.resolveToStrings()).containsExactly("test1", "test2");
	}

	@Test
	public void resolveToBooleanWithBoolean() {
		ResolvedExpression resolved = ResolvedExpression.of(true);
		assertThat(resolved.resolveToBoolean().value()).isPresent().contains(true);
	}

	@Test
	public void resolveToBooleanWithInvalidBooleanString() {
		ResolvedExpression resolved = ResolvedExpression.of("not a boolean");
		assertThat(resolved.resolveToBoolean().value()).isPresent().contains(false);
	}

	@Test
	public void resolveToBooleanWithString() {
		ResolvedExpression resolved = ResolvedExpression.of("true");
		assertThat(resolved.resolveToBoolean().value()).isPresent().contains(true);
	}

	@Test
	public void resolveToBooleanWithNonBoolean() {
		ResolvedExpression resolved = ResolvedExpression.of(123);
		assertThat(resolved.resolveToBoolean().exception()).isPresent();
		assertThat(resolved.resolveToBoolean().exception().get()).extracting("message")
			.isEqualTo("Resolved to [class java.lang.Integer] for [123]");
	}

	@Test
	public void resolveToIntegerWithString() {
		ResolvedExpression resolved = ResolvedExpression.of("123");
		assertThat(resolved.resolveToInteger().value()).isPresent().contains(123);
	}

	@Test
	public void resolveToIntegerWithNumber() {
		ResolvedExpression resolved = ResolvedExpression.of(123);
		assertThat(resolved.resolveToInteger().value()).isPresent().contains(123);
	}

	@Test
	public void resolveToIntegerWithNonStringNumber() {
		ResolvedExpression resolved = ResolvedExpression.of("testString");
		assertThat(resolved.resolveToInteger().exception()).isPresent();
		assertThat(resolved.resolveToInteger().exception().get()).extracting("message")
			.isEqualTo("Resolved to [class java.lang.String] for [testString]");
	}

	@Test
	public void resolveToIntegerWithNonNumber() {
		record NonNumber() {
		}
		ResolvedExpression resolved = ResolvedExpression.of(new NonNumber());
		assertThat(resolved.resolveToInteger().exception()).isPresent();
		assertThat(resolved.resolveToInteger().exception().get()).extracting("message")
			.isEqualTo(
					"Resolved to [class org.springframework.pulsar.core.ResolvedExpressionTest$1NonNumber] for [NonNumber[]]");
	}

	@Test
	public void getReturnsResolvedValue() {
		Object testObject = new Object();
		ResolvedExpression resolved = ResolvedExpression.of(testObject);
		assertThat(resolved.get()).isEqualTo(testObject);
	}

	@Test
	public void resolveToStringsWithNonStringArray() {
		ResolvedExpression resolved = ResolvedExpression.of(new Object[] { "string", 123, true });
		assertThatThrownBy(resolved::resolveToStrings).isInstanceOf(ResolutionException.class)
			.hasMessageContaining("Can't resolve '[Ljava.lang.Object;");
	}

	@Test
	public void resolveToIntegerWithInvalidString() {
		ResolvedExpression resolved = ResolvedExpression.of("not a number");
		assertThat(resolved.resolveToInteger().exception()).isPresent();
		assertThat(resolved.resolveToInteger().exception().get()).extracting("message")
			.isEqualTo("Resolved to [class java.lang.String] for [not a number]");
	}

}

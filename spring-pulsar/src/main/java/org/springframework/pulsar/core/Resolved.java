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

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.springframework.lang.Nullable;

/**
 * A resolved value or an exception if it could not be resolved.
 *
 * @param <T> the resolved type
 * @author Christophe Bornet
 * @author Chris Bono
 * @author Jonas Geiregat
 */
public final class Resolved<T> {

	@Nullable
	private final T value;

	@Nullable
	private final RuntimeException exception;

	private Resolved(@Nullable T value, @Nullable RuntimeException exception) {
		this.value = value;
		this.exception = exception;
	}

	/**
	 * Factory method to create a {@code Resolved} when resolution succeeds.
	 * @param value the non-{@code null} resolved value
	 * @param <T> the type of the value
	 * @return a {@code Resolved} containing the resolved value
	 */
	public static <T> Resolved<T> of(@Nullable T value) {
		return new Resolved<>(value, null);
	}

	/**
	 * Factory method to create a {@code Resolved} when resolution fails.
	 * @param reason the non-{@code null} reason the resolution failed
	 * @param <T> the type of the value
	 * @return a {@code Resolved} containing an {@link IllegalArgumentException} with the
	 * reason for the failure
	 */
	public static <T> Resolved<T> failed(String reason) {
		return new Resolved<>(null, new IllegalArgumentException(reason));
	}

	/**
	 * Factory method to create a {@code Resolved} when resolution fails.
	 * @param reason the non-{@code null} reason the resolution failed
	 * @param <T> the type of the value
	 * @return a {@code Resolved} containing the reason for the failure
	 */
	public static <T> Resolved<T> failed(RuntimeException reason) {
		return new Resolved<>(null, reason);
	}

	/**
	 * Gets the optional resolved value.
	 * @return an optional with the resolved value or empty if failed to resolve
	 * @deprecated Use {@link #value()} instead
	 */
	@Deprecated(since = "1.1.0", forRemoval = true)
	public Optional<T> get() {
		return value();
	}

	/**
	 * Gets the resolved value.
	 * @return an optional with the resolved value or empty if failed to resolve
	 */
	public Optional<T> value() {
		return Optional.ofNullable(this.value);
	}

	/**
	 * Gets the exception that may have occurred during resolution.
	 * @return an optional with the resolution exception or empty if no error occurred
	 */
	public Optional<RuntimeException> exception() {
		return Optional.ofNullable(this.exception);
	}

	/**
	 * Performs the given action with the resolved value if a value was resolved and no
	 * exception occurred.
	 * @param action the action to be performed
	 */
	public void ifResolved(Consumer<? super T> action) {
		if (this.value != null && this.exception == null) {
			action.accept(this.value);
		}
	}

	/**
	 * Performs the given action with the resolved value if a non-{@code null} value was
	 * resolved and no exception occurred. Otherwise, if an exception occurred then the
	 * provided error action is performed with the exception.
	 * @param action the action to be performed
	 * @param errorAction the error action to be performed
	 */
	public void ifResolvedOrElse(Consumer<? super T> action, Consumer<RuntimeException> errorAction) {
		if (this.value != null && this.exception == null) {
			action.accept(this.value);
		}
		else if (this.exception != null) {
			errorAction.accept(this.exception);
		}
	}

	/**
	 * Returns the resolved value if a value was resolved and no exception occurred,
	 * otherwise throws the resolution exception back to the caller.
	 * @return the resolved value if a value was resolved and no exception occurred
	 * @throws RuntimeException if an exception occurred during resolution
	 */
	public T orElseThrow() {
		if (this.value == null && this.exception != null) {
			throw this.exception;
		}
		return this.value;
	}

	/**
	 * Returns the resolved value if a value was resolved and no exception occurred,
	 * otherwise wraps the resolution exception with the provided error message and throws
	 * back to the caller.
	 * @param wrappingErrorMessage additional context to add to the wrapped exception
	 * @return the resolved value if a value was resolved and no exception occurred
	 * @throws RuntimeException wrapping the resolution exception if an exception occurred
	 * during resolution
	 */
	public T orElseThrow(Supplier<String> wrappingErrorMessage) {
		if (this.value == null && this.exception != null) {
			throw new RuntimeException(wrappingErrorMessage.get(), this.exception);
		}
		return this.value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Resolved<?> resolved = (Resolved<?>) o;
		return Objects.equals(this.value, resolved.value) && Objects.equals(this.exception, resolved.exception);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.value, this.exception);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", Resolved.class.getSimpleName() + "[", "]").add("value=" + this.value)
			.add("exception=" + this.exception)
			.toString();
	}

}

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

import java.util.Optional;
import java.util.function.Consumer;

import org.springframework.lang.Nullable;

/**
 * A resolved value or an exception if it could not be resolved.
 *
 * @param <T> the resolved type
 * @author Christophe Bornet
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

	public static <T> Resolved<T> of(T value) {
		return new Resolved<T>(value, null);
	}

	public static <T> Resolved<T> failed(String reason) {
		return new Resolved<T>(null, new IllegalArgumentException(reason));
	}

	public static <T> Resolved<T> failed(RuntimeException e) {
		return new Resolved<T>(null, e);
	}

	public Optional<T> get() {
		return Optional.ofNullable(this.value);
	}

	public void ifResolved(Consumer<? super T> action) {
		if (this.value != null) {
			action.accept(this.value);
		}
	}

	public T orElseThrow() {
		if (this.value == null && this.exception != null) {
			throw this.exception;
		}
		return this.value;
	}

}

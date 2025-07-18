/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.pulsar.support;

import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

/**
 * Chained utility methods to simplify some Java repetitive code. Obtain a reference to
 * the singleton {@link #INSTANCE} and then chain calls to the utility methods.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public final class JavaUtils {

	/**
	 * The singleton instance of this utility class.
	 */
	public static final JavaUtils INSTANCE = new JavaUtils();

	private JavaUtils() {
	}

	/**
	 * Invoke {@link Consumer#accept(Object)} with the value if it is not null.
	 * @param value the value.
	 * @param consumer the consumer.
	 * @param <T> the value type.
	 * @return this.
	 */
	@SuppressWarnings("NullAway")
	public <T> JavaUtils acceptIfNotNull(@Nullable T value, Consumer<T> consumer) {
		if (value != null) {
			consumer.accept(value);
		}
		return this;
	}

	/**
	 * Determine if the specified class is a Lambda.
	 * @param clazz the class to check
	 * @return whether the specified class is a Lambda
	 */
	public boolean isLambda(Class<?> clazz) {
		return clazz.isSynthetic() && clazz.getName().contains("$$Lambda") && !clazz.isAnonymousClass();
	}

}

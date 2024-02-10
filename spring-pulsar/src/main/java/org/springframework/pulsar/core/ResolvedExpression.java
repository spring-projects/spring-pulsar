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

import java.util.ArrayList;
import java.util.List;

/**
 * A resolved {@link ExpressionResolver} expression.
 *
 * @author Jonas Geiregat
 */
public final class ResolvedExpression {

	private static final String RESOLVED_TO_LEFT = "Resolved to [";

	private static final String RIGHT_FOR_LEFT = "] for [";

	private final Object resolvedValue;

	private ResolvedExpression(Object resolvedValue) {
		this.resolvedValue = resolvedValue;
	}

	public static ResolvedExpression of(Object value) {
		return new ResolvedExpression(value);
	}

	/**
	 * Resolve resulted resolution to a {@code String}.
	 * @return the {@link Resolved} value as a {@code String}
	 */
	public Resolved<String> resolveToString() {
		if (this.resolvedValue instanceof String) {
			return Resolved.of((String) this.resolvedValue);
		}
		else if (this.resolvedValue != null) {
			return Resolved
				.failed(RESOLVED_TO_LEFT + this.resolvedValue.getClass() + RIGHT_FOR_LEFT + this.resolvedValue + "]");
		}
		return Resolved.of(null);
	}

	/**
	 * Resolve resulted resolution to a {@code List<String>}.
	 * @return the {@link Resolved} value as a {@code List<String>}
	 */
	public List<String> resolveToStrings() {
		return doResolveToStrings(this.resolvedValue);
	}

	/**
	 * Resolve resulted resolution to a {@code Boolean}.
	 * @return the {@link Resolved} value as a {@code Boolean}
	 */
	public Resolved<Boolean> resolveToBoolean() {
		Boolean result = null;
		if (this.resolvedValue instanceof Boolean) {
			result = (Boolean) this.resolvedValue;
		}
		else if (this.resolvedValue instanceof String) {
			result = Boolean.parseBoolean((String) this.resolvedValue);
		}
		else if (this.resolvedValue != null) {
			return Resolved
				.failed(RESOLVED_TO_LEFT + this.resolvedValue.getClass() + RIGHT_FOR_LEFT + this.resolvedValue + "]");
		}
		return Resolved.of(result);
	}

	/**
	 * Resolve resulted resolution to a {@code Integer}.
	 * @return the {@link Resolved} value as a {@code Integer}
	 */
	public Resolved<Integer> resolveToInteger() {
		Integer result = null;
		if (this.resolvedValue instanceof String) {
			try {
				result = Integer.parseInt((String) this.resolvedValue);
			}
			catch (NumberFormatException ex) {
				return Resolved.failed(
						RESOLVED_TO_LEFT + this.resolvedValue.getClass() + RIGHT_FOR_LEFT + this.resolvedValue + "]");
			}
		}
		else if (this.resolvedValue instanceof Number) {
			result = ((Number) this.resolvedValue).intValue();
		}
		else {
			return Resolved
				.failed(RESOLVED_TO_LEFT + this.resolvedValue.getClass() + RIGHT_FOR_LEFT + this.resolvedValue + "]");
		}
		return Resolved.of(result);
	}

	/**
	 * Get the resolved value regardless.
	 * @return the resolved value
	 */
	public Object get() {
		return this.resolvedValue;
	}

	@SuppressWarnings("unchecked")
	private List<String> doResolveToStrings(Object value) {
		if (value instanceof String[]) {
			var result = new ArrayList<String>();
			for (Object object : (String[]) value) {
				result.addAll(doResolveToStrings(object));
			}
			return result;
		}
		else if (value instanceof String) {
			return List.of((String) value);
		}
		else if (value instanceof Iterable) {
			var result = new ArrayList<String>();
			for (Object object : (Iterable<Object>) value) {
				result.addAll(doResolveToStrings(object));
			}
			return result;
		}
		else {
			throw new ResolutionException("Can't resolve '%s' as a String".formatted(value));
		}
	}

}

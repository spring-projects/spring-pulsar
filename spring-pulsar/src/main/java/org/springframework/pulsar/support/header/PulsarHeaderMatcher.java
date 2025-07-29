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

package org.springframework.pulsar.support.header;

import java.util.Locale;
import java.util.Set;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;
import org.springframework.util.PatternMatchUtils;

/**
 * Defines the contract for matching message headers.
 *
 * <p>
 * Used by the header mapper to determine whether an incoming or outgoing message header
 * should be included in the target.
 *
 * @author Chris Bono
 */
public interface PulsarHeaderMatcher {

	/**
	 * Determine if the header matches.
	 * @param headerName the header name
	 * @return whether the header matches
	 */
	boolean matchHeader(String headerName);

	/**
	 * Determine if this matcher is a negative matcher where a match means that the header
	 * should <strong>not</strong> be included.
	 * @return whether this matcher is a negative matcher
	 */
	boolean isNegated();

	/**
	 * A matcher that never matches a set of headers.
	 */
	class NeverMatch implements PulsarHeaderMatcher {

		private final Set<String> neverMatchHeaders;

		public NeverMatch(String... headers) {
			Assert.notEmpty(headers, "headers must contain at least 1 non-null entry");
			Assert.noNullElements(headers, "headers must not contain null entries");
			this.neverMatchHeaders = Set.of(headers);
		}

		@Override
		public boolean matchHeader(String headerName) {
			return this.neverMatchHeaders.contains(headerName);
		}

		@Override
		public boolean isNegated() {
			return true;
		}

	}

	/**
	 * A pattern-based header matcher that matches if the specified header matches the
	 * specified simple pattern.
	 *
	 * @see PatternMatchUtils#simpleMatch(String, String)
	 */
	class PatternMatch implements PulsarHeaderMatcher {

		private static final LogAccessor LOGGER = new LogAccessor(PatternMatch.class);

		private final String pattern;

		private final boolean negate;

		public static PatternMatch fromPatternString(String pattern) {
			return new PatternMatch(pattern.startsWith("!") ? pattern.substring(1) : pattern, pattern.startsWith("!"));
		}

		public PatternMatch(String pattern, boolean negate) {
			Assert.notNull(pattern, "Pattern must not be null");
			this.pattern = pattern.toLowerCase(Locale.ROOT);
			this.negate = negate;
		}

		@Override
		public boolean matchHeader(String headerName) {
			if (!PatternMatchUtils.simpleMatch(this.pattern, headerName.toLowerCase(Locale.ROOT))) {
				return false;
			}
			LOGGER.debug(() -> "headerName=[%s] WILL %s be mapped, matched pattern=%s".formatted(headerName,
					this.negate ? "NOT " : "", toPatternString()));
			return true;
		}

		@Override
		public boolean isNegated() {
			return this.negate;
		}

		public String toPatternString() {
			return (this.negate ? "!" : "") + this.pattern;
		}

	}

}

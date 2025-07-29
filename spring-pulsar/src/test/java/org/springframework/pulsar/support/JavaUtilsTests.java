/*
 * Copyright 2024-present the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JavaUtils}.
 */
public class JavaUtilsTests {

	@Nested
	class IsLambdaApi {

		@Test
		void returnsTrueForLambda() {
			Consumer<String> lambdaConsumer = (__) -> {
			};
			assertThat(JavaUtils.INSTANCE.isLambda(lambdaConsumer.getClass())).isTrue();
		}

		@Test
		void returnsFalseForTopLevelClass() {
			assertThat(JavaUtils.INSTANCE.isLambda(String.class)).isFalse();
		}

		@Test
		void returnsFalseForStaticNestedClass() {
			assertThat(JavaUtils.INSTANCE.isLambda(StaticNestedClass.class)).isFalse();
		}

		@Test
		void returnsFalseForNonLambdaConsumer() {
			assertThat(JavaUtils.INSTANCE.isLambda(NonLambdaConsumer.class)).isFalse();
		}

		@Test
		void returnsFalseForAnonymousConsumer() {
			var anonymousConsumer = new Consumer<String>() {
				@Override
				public void accept(String s) {
				}
			};
			assertThat(JavaUtils.INSTANCE.isLambda(anonymousConsumer.getClass())).isFalse();
		}

		static class StaticNestedClass {

		}

		static class NonLambdaConsumer implements Consumer<String> {

			@Override
			public void accept(String s) {
			}

		}

	}

}

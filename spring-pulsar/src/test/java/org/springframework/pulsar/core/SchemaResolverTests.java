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

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;

/**
 * Unit tests for {@link SchemaResolver} default methods.
 *
 * @author Chris Bono
 */
class SchemaResolverTests {

	private TestSchemaResolver resolver = new TestSchemaResolver();

	@Nested
	class ResolveBySchemaTypeAndSchemaInfo {

		@Test
		void primitiveSchemaDoesNotRequireMessageType() {
			resolver.resolveSchema(SchemaType.STRING, null, null);
			verify(resolver.getMock()).resolveSchema(SchemaType.STRING, null);
		}

		@Test
		void structSchemaWithMessageType() {
			resolver.resolveSchema(SchemaType.JSON, Foo.class, null);
			verify(resolver.getMock()).resolveSchema(eq(SchemaType.JSON), eq(ResolvableType.forClass(Foo.class)));
		}

		@Test
		void structSchemasRequireMessageType() {
			assertThatIllegalArgumentException()
				.isThrownBy(() -> resolver.resolveSchema(SchemaType.JSON, null, null).orElseThrow())
				.withMessage("messageType must be specified for JSON schema type");
		}

		@Test
		void keyValueSchemaType() {
			resolver.resolveSchema(SchemaType.KEY_VALUE, Foo.class, String.class);
			verify(resolver.getMock()).resolveSchema(SchemaType.KEY_VALUE,
					ResolvableType.forClassWithGenerics(KeyValue.class, String.class, Foo.class));
		}

		@Test
		void keyValueSchemaRequiresMessageKeyType() {
			assertThatIllegalArgumentException()
				.isThrownBy(() -> resolver.resolveSchema(SchemaType.KEY_VALUE, Foo.class, null).orElseThrow())
				.withMessage("messageKeyType must be specified for KEY_VALUE schema type");
		}

		@Test
		void keyValueSchemaRequiresMessageType() {
			assertThatIllegalArgumentException()
				.isThrownBy(() -> resolver.resolveSchema(SchemaType.KEY_VALUE, null, String.class).orElseThrow())
				.withMessage("messageType must be specified for KEY_VALUE schema type");
		}

		@Test
		void schemaTypeNoneWithNullMessageType() {
			resolver.resolveSchema(SchemaType.NONE, null, null);
			verify(resolver.getMock()).resolveSchema(SchemaType.NONE, null);
		}

		@Test
		void schemaTypeNoneWithMessageType() {
			resolver.resolveSchema(SchemaType.NONE, Foo.class, null);
			verify(resolver.getMock()).resolveSchema(SchemaType.NONE, ResolvableType.forClass(Foo.class));
		}

		@Test
		void schemaTypeNoneWithMessageTypeAndKeyType() {
			resolver.resolveSchema(SchemaType.NONE, Foo.class, String.class);
			verify(resolver.getMock()).resolveSchema(SchemaType.NONE,
					ResolvableType.forClassWithGenerics(KeyValue.class, String.class, Foo.class));
		}

	}

	private static final class TestSchemaResolver implements SchemaResolver {

		private final SchemaResolver delegate = mock(SchemaResolver.class);

		SchemaResolver getMock() {
			return this.delegate;
		}

		@Override
		public <T> Resolved<Schema<T>> resolveSchema(@Nullable Class<?> messageType, boolean returnDefault) {
			return delegate.resolveSchema(messageType, returnDefault);
		}

		@Override
		public <T> Resolved<Schema<T>> resolveSchema(SchemaType schemaType, @Nullable ResolvableType messageType) {
			return delegate.resolveSchema(schemaType, messageType);
		}

	}

	record Foo(String value) {
	}

	// @formatter:off
	record Bar<T>(T value) {
	}
	// @formatter:on

}

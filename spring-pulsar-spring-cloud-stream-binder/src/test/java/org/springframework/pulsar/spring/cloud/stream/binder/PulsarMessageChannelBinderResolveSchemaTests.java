/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.spring.cloud.stream.binder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import org.springframework.core.ResolvableType;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.Resolved;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.spring.cloud.stream.binder.provisioning.PulsarTopicProvisioner;

/**
 * Unit tests for {@link PulsarMessageChannelBinder#resolveSchema}.
 *
 * @author Chris Bono
 */
public class PulsarMessageChannelBinderResolveSchemaTests {

	private SchemaResolver resolver = mock(SchemaResolver.class);

	@SuppressWarnings("unchecked")
	private PulsarMessageChannelBinder binder = new PulsarMessageChannelBinder(mock(PulsarTopicProvisioner.class),
			mock(PulsarTemplate.class), mock(PulsarConsumerFactory.class), resolver);

	@ParameterizedTest
	@EnumSource(mode = Mode.MATCH_NONE, names = "^(AUTO.*|AVRO|JSON|KEY_VALUE|NONE|PROTOBUF.*)$")
	void primitiveSchemaTypes(SchemaType schemaType) {
		doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(schemaType, null);
		binder.resolveSchema(schemaType, null, null, null);
		verify(resolver).resolveSchema(schemaType, null);
	}

	@ParameterizedTest
	@EnumSource(mode = Mode.MATCH_ALL, names = "^(JSON|AVRO|PROTOBUF)$")
	void structSchemaTypes(SchemaType schemaType) {
		doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(eq(schemaType), any());
		binder.resolveSchema(schemaType, Foo.class, null, null);
		verify(resolver).resolveSchema(schemaType, ResolvableType.forClass(Foo.class));
	}

	@Test
	void keyValueSchemaType() {
		doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(eq(SchemaType.KEY_VALUE), any());
		binder.resolveSchema(SchemaType.KEY_VALUE, null, Foo.class, Bar.class);
		verify(resolver).resolveSchema(SchemaType.KEY_VALUE,
				ResolvableType.forClassWithGenerics(KeyValue.class, Foo.class, Bar.class));
	}

	@ParameterizedTest
	@EnumSource(mode = Mode.MATCH_ALL, names = "^(JSON|AVRO|PROTOBUF)$")
	void structSchemaTypesRequireMessageType(SchemaType schemaType) {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> binder.resolveSchema(schemaType, null, null, null))
				.withMessage("'message-type' required for 'schema-type' " + schemaType.name());
	}

	@Test
	void keyValueSchemaTypeRequiresKeyAndValueTypes() {
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> binder.resolveSchema(SchemaType.KEY_VALUE, null, null, null))
				.withMessage("'message-key-type' required for 'schema-type' KEY_VALUE");
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> binder.resolveSchema(SchemaType.KEY_VALUE, null, null, Bar.class))
				.withMessage("'message-key-type' required for 'schema-type' KEY_VALUE");
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> binder.resolveSchema(SchemaType.KEY_VALUE, null, Foo.class, null))
				.withMessage("'message-value-type' required for 'schema-type' KEY_VALUE");
	}

	@Nested
	class SchemaTypeNone {

		@Test
		void withMesssageType() {
			doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(eq(SchemaType.NONE), any());
			binder.resolveSchema(SchemaType.NONE, Foo.class, null, null);
			verify(resolver).resolveSchema(SchemaType.NONE, ResolvableType.forClass(Foo.class));
		}

		@Test
		void withKeyAndValueTypes() {
			doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(eq(SchemaType.NONE), any());
			binder.resolveSchema(SchemaType.NONE, null, Foo.class, Bar.class);
			verify(resolver).resolveSchema(SchemaType.NONE,
					ResolvableType.forClassWithGenerics(KeyValue.class, Foo.class, Bar.class));
		}

		@Test
		void withMessageTypeAndKeyAndValueTypes() {
			doReturn(Resolved.of(Schema.STRING)).when(resolver).resolveSchema(eq(SchemaType.NONE), any());
			binder.resolveSchema(SchemaType.NONE, Foo.class, String.class, Bar.class);
			verify(resolver).resolveSchema(SchemaType.NONE, ResolvableType.forClass(Foo.class));
		}

		@Test
		void withOnlyKeyType() {
			assertThatExceptionOfType(IllegalArgumentException.class)
					.isThrownBy(() -> binder.resolveSchema(SchemaType.NONE, null, Foo.class, null)).withMessage(
							"'message-type' OR ('message-key-type' AND 'message-value-type') required for 'schema-type' NONE");
		}

		@Test
		void withOnlyValueType() {
			assertThatExceptionOfType(IllegalArgumentException.class)
					.isThrownBy(() -> binder.resolveSchema(SchemaType.NONE, null, null, Foo.class)).withMessage(
							"'message-type' OR ('message-key-type' AND 'message-value-type') required for 'schema-type' NONE");

		}

	}

	record Foo(String value) {
	}

	record Bar(String value) {
	}

}

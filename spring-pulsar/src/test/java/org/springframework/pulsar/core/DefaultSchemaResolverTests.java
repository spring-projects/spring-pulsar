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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.core.ResolvableType;
import org.springframework.pulsar.listener.Proto;
import org.springframework.pulsar.listener.Proto.Person;

/**
 * Unit tests for {@link DefaultSchemaResolver}.
 *
 * @author Chris Bono
 */
class DefaultSchemaResolverTests {

	private DefaultSchemaResolver resolver = new DefaultSchemaResolver();

	@Nested
	class CustomSchemaMappingsAPI {

		@Test
		void noMappingsByDefault() {
			assertThat(resolver.getCustomSchemaMappings()).asInstanceOf(InstanceOfAssertFactories.MAP).isEmpty();
		}

		@Test
		void addMappings() {
			Schema<?> previouslyMappedSchema = resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
			assertThat(previouslyMappedSchema).isNull();
			assertThat(resolver.getCustomSchemaMappings()).asInstanceOf(InstanceOfAssertFactories.MAP)
					.containsEntry(Foo.class, Schema.STRING);
			previouslyMappedSchema = resolver.addCustomSchemaMapping(Foo.class, Schema.BOOL);
			assertThat(previouslyMappedSchema).isEqualTo(Schema.STRING);
			assertThat(resolver.getCustomSchemaMappings()).asInstanceOf(InstanceOfAssertFactories.MAP)
					.containsEntry(Foo.class, Schema.BOOL);
		}

		@Test
		void removeMappings() {
			Schema<?> previouslyMappedSchema = resolver.removeCustomMapping(Foo.class);
			assertThat(previouslyMappedSchema).isNull();
			resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
			previouslyMappedSchema = resolver.removeCustomMapping(Foo.class);
			assertThat(previouslyMappedSchema).isEqualTo(Schema.STRING);
			assertThat(resolver.getCustomSchemaMappings()).asInstanceOf(InstanceOfAssertFactories.MAP).isEmpty();
		}

	}

	@Nested
	class SchemaByMessageInstance {

		@ParameterizedTest
		@MethodSource("primitiveTypeMessagesProvider")
		<T> void primitiveTypeMessages(T message, Schema<T> expectedSchema) {
			assertThat(resolver.getSchema(message)).isEqualTo(expectedSchema);
		}

		static Stream<Arguments> primitiveTypeMessagesProvider() {
			// @formatter:off
			return Stream.of(
					arguments("foo".getBytes(), Schema.BYTES),
					arguments(ByteBuffer.wrap("foo".getBytes()), Schema.BYTEBUFFER),
					arguments(ByteBuffer.allocateDirect(10), Schema.BYTEBUFFER),
					arguments("foo", Schema.STRING),
					arguments(Boolean.TRUE, Schema.BOOL),
					arguments(Byte.valueOf("0"), Schema.INT8),
					arguments((byte) 0, Schema.INT8),
					arguments(Short.valueOf("0"), Schema.INT16),
					arguments((short) 0, Schema.INT16),
					arguments(Integer.valueOf("0"), Schema.INT32),
					arguments(0, Schema.INT32),
					arguments(Long.valueOf("0"), Schema.INT64),
					arguments(0L, Schema.INT64),
					arguments(Float.valueOf("2.4"), Schema.FLOAT),
					arguments(2.5f, Schema.FLOAT),
					arguments(Double.valueOf("2.2"), Schema.DOUBLE),
					arguments(2.3d, Schema.DOUBLE),
					arguments(new Date(), Schema.DATE),
					arguments(new Time(System.currentTimeMillis()), Schema.TIME),
					arguments(new Timestamp(System.currentTimeMillis()), Schema.TIMESTAMP),
					arguments(Instant.now(), Schema.INSTANT),
					arguments(LocalDate.now(), Schema.LOCAL_DATE),
					arguments(LocalDateTime.now(), Schema.LOCAL_DATE_TIME),
					arguments(LocalTime.NOON, Schema.LOCAL_TIME)
			);
			// @formatter:on
		}

		@Test
		void customTypeMessages() {
			Schema<?> fooSchema = Schema.AVRO(Foo.class);
			resolver.addCustomSchemaMapping(Foo.class, fooSchema);
			resolver.addCustomSchemaMapping(Bar.class, Schema.STRING);
			assertThat(resolver.getSchema(new Foo("foo1"))).isSameAs(fooSchema);
			assertThat(resolver.getSchema(new Bar<>("bar1"))).isEqualTo(Schema.STRING);
			assertThat(resolver.getSchema(new Zaa("zaa1")).getSchemaInfo())
					.isEqualTo(Schema.JSON(Zaa.class).getSchemaInfo());
		}

	}

	@Nested
	class SchemaByMessageType {

		@ParameterizedTest
		@MethodSource("primitiveMessageTypesProvider")
		<T> void primitiveMessageTypes(Class<?> messageType, Schema<T> expectedSchema) {
			assertThat(resolver.getSchema(messageType)).isEqualTo(expectedSchema);
		}

		static Stream<Arguments> primitiveMessageTypesProvider() {
			// @formatter:off
			return Stream.of(
					arguments(byte[].class, Schema.BYTES),
					arguments(ByteBuffer.class, Schema.BYTEBUFFER),
					arguments(ByteBuffer.wrap("foo".getBytes()).getClass(), Schema.BYTEBUFFER),
					arguments(ByteBuffer.allocateDirect(10).getClass(), Schema.BYTEBUFFER),
					arguments(String.class, Schema.STRING),
					arguments(Boolean.class, Schema.BOOL),
					arguments(boolean.class, Schema.BOOL),
					arguments(Byte.class, Schema.INT8),
					arguments(byte.class, Schema.INT8),
					arguments(Short.class, Schema.INT16),
					arguments(short.class, Schema.INT16),
					arguments(Integer.class, Schema.INT32),
					arguments(int.class, Schema.INT32),
					arguments(Long.class, Schema.INT64),
					arguments(long.class, Schema.INT64),
					arguments(Date.class, Schema.DATE),
					arguments(Time.class, Schema.TIME),
					arguments(Timestamp.class, Schema.TIMESTAMP),
					arguments(Float.class, Schema.FLOAT),
					arguments(float.class, Schema.FLOAT),
					arguments(Double.class, Schema.DOUBLE),
					arguments(double.class, Schema.DOUBLE),
					arguments(Instant.class, Schema.INSTANT),
					arguments(LocalDate.class, Schema.LOCAL_DATE),
					arguments(LocalDateTime.class, Schema.LOCAL_DATE_TIME),
					arguments(LocalTime.class, Schema.LOCAL_TIME)
			);
			// @formatter:on
		}

		@Test
		void customMessageTypes() {
			assertThat(resolver.getSchema(Foo.class, false)).isNull();
			assertThat(resolver.getSchema(Foo.class, true).getSchemaInfo())
					.isEqualTo(Schema.JSON(Foo.class).getSchemaInfo());
			resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
			assertThat(resolver.getSchema(Foo.class, false)).isEqualTo(Schema.STRING);
			assertThat(resolver.getSchema(Bar.class, false)).isNull();
			assertThat(resolver.getSchema(Bar.class, true)).isEqualTo(Schema.BYTES);
		}

	}

	@Nested
	class SchemaBySchemaTypeAndMessageType {

		@ParameterizedTest
		@MethodSource("primitiveSchemasProvider")
		<T> void primitiveSchemas(SchemaType schemaType, Schema<T> expectedSchema) {
			assertThat(resolver.getSchema(schemaType, null)).isEqualTo(expectedSchema);
		}

		static Stream<Arguments> primitiveSchemasProvider() {
			// @formatter:off
			return Stream.of(
					arguments(SchemaType.STRING, Schema.STRING),
					arguments(SchemaType.BOOLEAN, Schema.BOOL),
					arguments(SchemaType.INT8, Schema.INT8),
					arguments(SchemaType.INT16, Schema.INT16),
					arguments(SchemaType.INT32, Schema.INT32),
					arguments(SchemaType.INT64, Schema.INT64),
					arguments(SchemaType.FLOAT, Schema.FLOAT),
					arguments(SchemaType.DOUBLE, Schema.DOUBLE),
					arguments(SchemaType.DATE, Schema.DATE),
					arguments(SchemaType.TIME, Schema.TIME),
					arguments(SchemaType.TIMESTAMP, Schema.TIMESTAMP),
					arguments(SchemaType.BYTES, Schema.BYTES),
					arguments(SchemaType.INSTANT, Schema.INSTANT),
					arguments(SchemaType.LOCAL_DATE, Schema.LOCAL_DATE),
					arguments(SchemaType.LOCAL_TIME, Schema.LOCAL_TIME),
					arguments(SchemaType.LOCAL_DATE_TIME, Schema.LOCAL_DATE_TIME)
					);
			// @formatter:on
		}

		@Test
		void structSchemas() {
			assertThat(resolver.getSchema(SchemaType.JSON, ResolvableType.forType(Foo.class)))
					.isInstanceOf(JSONSchema.class)
					.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Foo.class));
			assertThat(resolver.getSchema(SchemaType.AVRO, ResolvableType.forType(Foo.class)))
					.isInstanceOf(AvroSchema.class)
					.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Foo.class));
			assertThat(resolver.getSchema(SchemaType.PROTOBUF, ResolvableType.forType(Person.class)))
					.isInstanceOf(ProtobufSchema.class)
					.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Proto.Person.class));
			ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, String.class, Integer.class);
			assertThat(resolver.getSchema(SchemaType.KEY_VALUE, kvType))
					.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class)).satisfies((keyValueSchema -> {
						assertThat(keyValueSchema.getKeySchema()).isEqualTo(Schema.STRING);
						assertThat(keyValueSchema.getValueSchema()).isEqualTo(Schema.INT32);
						assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
					}));
		}

		@ParameterizedTest
		@EnumSource(value = SchemaType.class, names = { "JSON", "AVRO", "PROTOBUF", "KEY_VALUE" })
		void structSchemasRequireMessageType(SchemaType schemaType) {
			assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> resolver.getSchema(schemaType, null))
					.withMessage("messageType must be specified for " + schemaType.name());
		}

		@ParameterizedTest
		@EnumSource(value = SchemaType.class, names = { "PROTOBUF_NATIVE", "AUTO", "AUTO_CONSUME", "AUTO_PUBLISH" })
		void unsupportedSchemaTypes(SchemaType unsupportedType) {
			assertThatExceptionOfType(IllegalArgumentException.class)
					.isThrownBy(() -> resolver.getSchema(unsupportedType, null))
					.withMessage("Unsupported schema type: " + unsupportedType.name());
		}

		private String sanitizedClassName(Class<?> clazz) {
			return clazz.getName().replace("$", ".");
		}

		@Nested
		class SchemaTypeNone {

			@Test
			void nullMessageType() {
				assertThat(resolver.getSchema(SchemaType.NONE, null)).isEqualTo(Schema.BYTES);
			}

			@Test
			void primitiveMessageType() {
				assertThat(resolver.getSchema(SchemaType.NONE, ResolvableType.forType(String.class)))
						.isEqualTo(Schema.STRING);
			}

			@Test
			void customMessageType() {
				assertThat(resolver.getSchema(SchemaType.NONE, ResolvableType.forType(Foo.class))).isNull();
				resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
				assertThat(resolver.getSchema(SchemaType.NONE, ResolvableType.forType(Foo.class)))
						.isEqualTo(Schema.STRING);
			}

			@Test
			void primitiveKeyValueMessageType() {
				ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, String.class,
						Integer.class);
				assertThat(resolver.getSchema(SchemaType.NONE, kvType))
						.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
						.satisfies((keyValueSchema -> {
							assertThat(keyValueSchema.getKeySchema()).isEqualTo(Schema.STRING);
							assertThat(keyValueSchema.getValueSchema()).isEqualTo(Schema.INT32);
							assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
						}));
			}

			@Test
			void customKeyValueMessageTypeDefaultsToJSONSchema() {
				ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, Foo.class, Zaa.class);
				assertThat(resolver.getSchema(SchemaType.NONE, kvType))
						.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
						.satisfies((keyValueSchema -> {
							assertThat(keyValueSchema.getKeySchema().getSchemaInfo())
									.isEqualTo(Schema.JSON(Foo.class).getSchemaInfo());
							assertThat(keyValueSchema.getValueSchema().getSchemaInfo())
									.isEqualTo(Schema.JSON(Zaa.class).getSchemaInfo());
							assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
						}));
			}

			@Test
			void customKeyValueMessageTypeWithCustomTypeMappings() {
				Schema<?> fooSchema = mock(Schema.class);
				Schema<?> barSchema = mock(Schema.class);
				resolver.addCustomSchemaMapping(Foo.class, fooSchema);
				resolver.addCustomSchemaMapping(Bar.class, barSchema);
				ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, Foo.class, Bar.class);
				assertThat(resolver.getSchema(SchemaType.NONE, kvType))
						.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
						.satisfies((keyValueSchema -> {
							assertThat(keyValueSchema.getKeySchema()).isSameAs(fooSchema);
							assertThat(keyValueSchema.getValueSchema()).isSameAs(barSchema);
							assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
						}));
			}

		}

	}

	record Foo(String value) {
	}

	record Bar<T> (T value) {
	}

	record Zaa(String value) {
	}

}

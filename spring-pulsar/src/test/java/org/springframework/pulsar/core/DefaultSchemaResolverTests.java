/*
 * Copyright 2023-2024 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
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
import org.springframework.pulsar.annotation.PulsarTypeMapping;
import org.springframework.pulsar.listener.Proto;
import org.springframework.pulsar.listener.Proto.Person;

/**
 * Unit tests for {@link DefaultSchemaResolver}.
 *
 * @author Chris Bono
 * @author Aleksei Arsenev
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
			assertThat(resolver.resolveSchema(message).orElseThrow()).isEqualTo(expectedSchema);
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
			assertThat(resolver.resolveSchema(new Foo("foo1")).orElseThrow()).isSameAs(fooSchema);
			assertThat(resolver.resolveSchema(new Bar<>("bar1")).orElseThrow()).isEqualTo(Schema.STRING);
			assertThat(resolver.resolveSchema(new Zaa("zaa1")).orElseThrow().getSchemaInfo())
				.isEqualTo(Schema.JSON(Zaa.class).getSchemaInfo());
		}

	}

	@Nested
	class SchemaByMessageType {

		@ParameterizedTest
		@MethodSource("primitiveMessageTypesProvider")
		<T> void primitiveMessageTypes(Class<?> messageType, Schema<T> expectedSchema) {
			assertThat(resolver.resolveSchema(messageType).orElseThrow()).isEqualTo(expectedSchema);
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
			assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> resolver.resolveSchema(Foo.class, false).orElseThrow());
			assertThat(resolver.resolveSchema(Foo.class, true).orElseThrow().getSchemaInfo())
				.isEqualTo(Schema.JSON(Foo.class).getSchemaInfo());
			resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
			assertThat(resolver.resolveSchema(Foo.class, false).orElseThrow()).isEqualTo(Schema.STRING);
			assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> resolver.resolveSchema(Bar.class, false).orElseThrow());
			assertThat(resolver.resolveSchema(Bar.class, true).orElseThrow()).isEqualTo(Schema.BYTES);
		}

		@Test
		void annotatedMessageType() {
			assertThat(resolver.resolveSchema(Zaz.class, false).orElseThrow()).isEqualTo(Schema.STRING);
		}

	}

	@Nested
	class SchemaBySchemaTypeAndMessageType {

		@ParameterizedTest
		@MethodSource("primitiveSchemasProvider")
		<T> void primitiveSchemas(SchemaType schemaType, Schema<T> expectedSchema) {
			assertThat(resolver.resolveSchema(schemaType, null).orElseThrow()).isEqualTo(expectedSchema);
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
			assertThat(resolver.resolveSchema(SchemaType.JSON, ResolvableType.forType(Foo.class)).orElseThrow())
				.isInstanceOf(JSONSchema.class)
				.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Foo.class));
			assertThat(resolver.resolveSchema(SchemaType.AVRO, ResolvableType.forType(Foo.class)).orElseThrow())
				.isInstanceOf(AvroSchema.class)
				.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Foo.class));
			assertThat(resolver.resolveSchema(SchemaType.PROTOBUF, ResolvableType.forType(Person.class)).orElseThrow())
				.isInstanceOf(ProtobufSchema.class)
				.hasFieldOrPropertyWithValue("schema.fullName", sanitizedClassName(Proto.Person.class));
			ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, String.class, Integer.class);
			assertThat(resolver.resolveSchema(SchemaType.KEY_VALUE, kvType).orElseThrow())
				.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
				.satisfies((keyValueSchema -> {
					assertThat(keyValueSchema.getKeySchema()).isEqualTo(Schema.STRING);
					assertThat(keyValueSchema.getValueSchema()).isEqualTo(Schema.INT32);
					assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
				}));
		}

		@Test
		void autoConsumeSchema() {
			assertThat(resolver.resolveSchema(SchemaType.AUTO_CONSUME, ResolvableType.forType(Foo.class)).orElseThrow())
				.isInstanceOf(AutoConsumeSchema.class);
			assertThat(
					resolver.resolveSchema(SchemaType.AUTO_CONSUME, ResolvableType.forType(String.class)).orElseThrow())
				.isInstanceOf(AutoConsumeSchema.class);
			assertThat(resolver.resolveSchema(SchemaType.AUTO_CONSUME, null).orElseThrow())
				.isInstanceOf(AutoConsumeSchema.class);
		}

		@ParameterizedTest
		@EnumSource(value = SchemaType.class, names = { "JSON", "AVRO", "PROTOBUF", "KEY_VALUE" })
		void structSchemasRequireMessageType(SchemaType schemaType) {
			assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> resolver.resolveSchema(schemaType, null).orElseThrow())
				.withMessage("messageType must be specified for " + schemaType.name());
		}

		@ParameterizedTest
		@EnumSource(value = SchemaType.class, names = { "PROTOBUF_NATIVE", "AUTO", "AUTO_PUBLISH" })
		void unsupportedSchemaTypes(SchemaType unsupportedType) {
			assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> resolver.resolveSchema(unsupportedType, null).orElseThrow())
				.withMessage("Unsupported schema type: " + unsupportedType.name());
		}

		private String sanitizedClassName(Class<?> clazz) {
			return clazz.getName().replace("$", ".");
		}

		@Nested
		class SchemaTypeNone {

			@Test
			void nullMessageType() {
				assertThat(resolver.resolveSchema(SchemaType.NONE, null).orElseThrow()).isEqualTo(Schema.BYTES);
			}

			@Test
			void primitiveMessageType() {
				assertThat(resolver.resolveSchema(SchemaType.NONE, ResolvableType.forType(String.class)).orElseThrow())
					.isEqualTo(Schema.STRING);
			}

			@Test
			void customMessageTypeDefaultsToJson() {
				assertThat(resolver.resolveSchema(SchemaType.NONE, ResolvableType.forType(Foo.class)).orElseThrow())
					.extracting(Schema::getSchemaInfo)
					.isEqualTo(Schema.JSON(Foo.class).getSchemaInfo());
			}

			@Test
			void customMessageTypeRespectsCustomMappings() {
				resolver.addCustomSchemaMapping(Foo.class, Schema.STRING);
				assertThat(resolver.resolveSchema(SchemaType.NONE, ResolvableType.forType(Foo.class)).orElseThrow())
					.isEqualTo(Schema.STRING);
			}

			@Test
			void primitiveKeyValueMessageType() {
				ResolvableType kvType = ResolvableType.forClassWithGenerics(KeyValue.class, String.class,
						Integer.class);
				assertThat(resolver.resolveSchema(SchemaType.NONE, kvType).orElseThrow())
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
				assertThat(resolver.resolveSchema(SchemaType.NONE, kvType).orElseThrow())
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
				assertThat(resolver.resolveSchema(SchemaType.NONE, kvType).orElseThrow())
					.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
					.satisfies((keyValueSchema -> {
						assertThat(keyValueSchema.getKeySchema()).isSameAs(fooSchema);
						assertThat(keyValueSchema.getValueSchema()).isSameAs(barSchema);
						assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
					}));
			}

		}

	}

	@Nested
	class SchemaByAnnotatedMessageType {

		@Test
		void annotatedMessageType() {
			resolver = spy(resolver);
			var resolvedSchema = resolver.resolveSchema(JsonMsgType.class, false).orElseThrow();
			assertThat(resolvedSchema).isInstanceOf(JSONSchema.class)
				.extracting("schema.fullName")
				.asString()
				.endsWith(JsonMsgType.class.getSimpleName());

			// verify added to custom mappings
			assertThat(resolver.getCustomSchemaMappings().get(JsonMsgType.class)).isSameAs(resolvedSchema);

			// verify subsequent calls skip resolution again
			assertThat(resolver.resolveSchema(JsonMsgType.class, false).orElseThrow()).isSameAs(resolvedSchema);
			verify(resolver, times(1)).getAnnotatedSchemaType(JsonMsgType.class);
		}

		@Test
		void annotatedMessageTypeKeyValue() {
			assertThat(resolver.resolveSchema(KeyValueMsgType.class, false).orElseThrow())
				.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class))
				.satisfies((keyValueSchema -> {
					assertThat(keyValueSchema.getKeySchema()).isEqualTo(Schema.STRING);
					assertThat(keyValueSchema.getValueSchema()).isInstanceOf(JSONSchema.class)
						.extracting("schema.fullName")
						.asString()
						.endsWith(KeyValueMsgType.class.getSimpleName());
					assertThat(keyValueSchema.getKeyValueEncodingType()).isEqualTo(KeyValueEncodingType.INLINE);
				}));
		}

		@Test
		void annotatedMessageTypeKeyValueMissingKeyInfo() {
			assertThatIllegalStateException()
				.isThrownBy(() -> resolver.resolveSchema(KeyValueMsgTypeNoKeyInfo.class, false).orElseThrow())
				.withMessage("messageKeyClass can not be Void.class when using KEY_VALUE schema type");
		}

		@Test
		void annotatedMessageTypeKeyValueMissingValueInfo() {
			assertThatIllegalStateException()
				.isThrownBy(() -> resolver.resolveSchema(KeyValueMsgTypeNoValueInfo.class, false).orElseThrow())
				.withMessage("messageValueSchemaType can not be NONE or KEY_VALUE when using KEY_VALUE schema type");
		}

		@Test
		void annotatedMessageTypeNoSchemaInfo() {
			assertThatIllegalArgumentException()
				.isThrownBy(() -> resolver.resolveSchema(NoSchemaInfoMsgType.class, false).orElseThrow())
				.withMessage("Schema not specified and no schema found for " + NoSchemaInfoMsgType.class);
		}

		@Test
		void annotationMappingIgnoredWhenFeatureDisabled() {
			resolver.usePulsarTypeMappingAnnotations(false);
			assertThatIllegalArgumentException()
				.isThrownBy(() -> resolver.resolveSchema(JsonMsgType.class, false).orElseThrow())
				.withMessage("Schema not specified and no schema found for " + JsonMsgType.class);
		}

		@Test
		void customMappingTakesPrecedenceOverAnnotationMapping() {
			resolver.addCustomSchemaMapping(JsonMsgType.class, Schema.STRING);
			assertThat(resolver.resolveSchema(JsonMsgType.class, false).orElseThrow()).isEqualTo(Schema.STRING);
		}

		@PulsarTypeMapping(schemaType = SchemaType.JSON)
		record JsonMsgType(String value) {
		}

		@PulsarTypeMapping(schemaType = SchemaType.KEY_VALUE, messageKeyType = String.class,
				messageValueSchemaType = SchemaType.JSON)
		record KeyValueMsgType(String key) {
		}

		@PulsarTypeMapping(schemaType = SchemaType.KEY_VALUE, messageValueSchemaType = SchemaType.JSON)
		record KeyValueMsgTypeNoKeyInfo(String key) {
		}

		@PulsarTypeMapping(schemaType = SchemaType.KEY_VALUE, messageKeyType = String.class)
		record KeyValueMsgTypeNoValueInfo(String key) {
		}

		@PulsarTypeMapping(topic = "ignore-topic")
		record NoSchemaInfoMsgType(String value) {
		}

	}

	record Foo(String value) {
	}

	record Bar<T>(T value) {
	}

	record Zaa(String value) {
	}

	@PulsarTypeMapping(schemaType = SchemaType.STRING)
	record Zaz(String value) {
	}

}

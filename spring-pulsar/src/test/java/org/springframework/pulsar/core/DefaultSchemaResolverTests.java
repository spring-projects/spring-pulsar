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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link DefaultSchemaResolver}.
 *
 * @author Chris Bono
 */
class DefaultSchemaResolverTests {

	@ParameterizedTest
	@MethodSource("schemaByMessageInstanceProvider")
	<T> void getSchemaByMessageInstance(T message, Schema<T> expectedSchema) {
		DefaultSchemaResolver resolver = new DefaultSchemaResolver();
		assertThat(resolver.getSchema(message)).isEqualTo(expectedSchema);
	}

	static Stream<Arguments> schemaByMessageInstanceProvider() {
		// @formatter:off
		return Stream.of(
				arguments("foo", Schema.STRING),
				arguments("foo".getBytes(), Schema.BYTES),
				arguments(Byte.valueOf("0"), Schema.INT8),
				arguments((byte) 0, Schema.INT8),
				arguments(Short.valueOf("0"), Schema.INT16),
				arguments((short) 0, Schema.INT16),
				arguments(Integer.valueOf("0"), Schema.INT32),
				arguments(0, Schema.INT32),
				arguments(Long.valueOf("0"), Schema.INT64),
				arguments(0L, Schema.INT64),
				arguments(Boolean.TRUE, Schema.BOOL),
				arguments(ByteBuffer.wrap("foo".getBytes()), Schema.BYTEBUFFER),
				arguments(ByteBuffer.allocateDirect(10), Schema.BYTEBUFFER),
				arguments(new Date(), Schema.DATE),
				arguments(Double.valueOf("2.2"), Schema.DOUBLE),
				arguments(2.3d, Schema.DOUBLE),
				arguments(Float.valueOf("2.4"), Schema.FLOAT),
				arguments(2.5f, Schema.FLOAT),
				arguments(Instant.now(), Schema.INSTANT),
				arguments(LocalDate.now(), Schema.LOCAL_DATE),
				arguments(LocalDateTime.now(), Schema.LOCAL_DATE_TIME),
				arguments(LocalTime.NOON, Schema.LOCAL_TIME)
		);
		// @formatter:on
	}

	@Test
	void getSchemaByMessageInstanceCustomTypes() {
		Schema<?> fooSchema = Schema.AVRO(Foo.class);
		Map<Class<?>, Schema<?>> customTypes = new HashMap<>();
		customTypes.put(Foo.class, fooSchema);
		customTypes.put(Bar.class, Schema.STRING);
		DefaultSchemaResolver resolver = new DefaultSchemaResolver(customTypes);
		assertThat(resolver.getSchema(new Foo("foo1"))).isSameAs(fooSchema);
		assertThat(resolver.getSchema(new Bar<>("bar1"))).isEqualTo(Schema.STRING);
		assertThat(resolver.getSchema(new Zaa("zaa1"))).isEqualTo(Schema.BYTES); // default
	}

	@ParameterizedTest
	@MethodSource("schemaByMessageTypeProvider")
	<T> void getSchemaByMessageType(Class<?> messageType, Schema<T> expectedSchema) {
		DefaultSchemaResolver resolver = new DefaultSchemaResolver();
		assertThat(resolver.getSchema(messageType)).isEqualTo(expectedSchema);
	}

	static Stream<Arguments> schemaByMessageTypeProvider() {
		// @formatter:off
		return Stream.of(
				arguments(String.class, Schema.STRING),
				arguments(byte[].class, Schema.BYTES),
				arguments(Byte.class, Schema.INT8),
				arguments(byte.class, Schema.INT8),
				arguments(Short.class, Schema.INT16),
				arguments(short.class, Schema.INT16),
				arguments(Integer.class, Schema.INT32),
				arguments(int.class, Schema.INT32),
				arguments(Long.class, Schema.INT64),
				arguments(long.class, Schema.INT64),
				arguments(Boolean.class, Schema.BOOL),
				arguments(boolean.class, Schema.BOOL),
				arguments(ByteBuffer.class, Schema.BYTEBUFFER),
				arguments(ByteBuffer.wrap("foo".getBytes()).getClass(), Schema.BYTEBUFFER),
				arguments(ByteBuffer.allocateDirect(10).getClass(), Schema.BYTEBUFFER),
				arguments(Date.class, Schema.DATE),
				arguments(Double.class, Schema.DOUBLE),
				arguments(double.class, Schema.DOUBLE),
				arguments(Float.class, Schema.FLOAT),
				arguments(float.class, Schema.FLOAT),
				arguments(Instant.class, Schema.INSTANT),
				arguments(LocalDate.class, Schema.LOCAL_DATE),
				arguments(LocalDateTime.class, Schema.LOCAL_DATE_TIME),
				arguments(LocalTime.class, Schema.LOCAL_TIME)
		);
		// @formatter:on
	}

	@Test
	void getSchemaByMessageTypeCustomTypes() {
		Schema<?> fooSchema = Schema.AVRO(Foo.class);
		Map<Class<?>, Schema<?>> customTypes = new HashMap<>();
		customTypes.put(Foo.class, fooSchema);
		customTypes.put(Bar.class, Schema.STRING);
		DefaultSchemaResolver resolver = new DefaultSchemaResolver(customTypes);
		assertThat(resolver.getSchema(Foo.class)).isSameAs(fooSchema);
		assertThat(resolver.getSchema(Bar.class)).isEqualTo(Schema.STRING);
		assertThat(resolver.getSchema(Zaa.class)).isEqualTo(Schema.BYTES); // default
	}

	@Test
	void getSchemaByMessageTypeWithoutDefaults() {
		DefaultSchemaResolver resolver = new DefaultSchemaResolver();
		assertThat(resolver.getSchema(Foo.class, false)).isNull();

		resolver = new DefaultSchemaResolver(Collections.singletonMap(Foo.class, Schema.STRING));
		assertThat(resolver.getSchema(Foo.class, false)).isEqualTo(Schema.STRING);
		assertThat(resolver.getSchema(Bar.class, false)).isNull();
	}

	record Foo(String value) {
	}

	record Bar<T> (T value) {
	}

	record Zaa(String value) {
	}

}

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

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Factory to create schema definition {@link SchemaDefinitionBuilder builders} that
 * provide schema definitions that use custom object mappers when de/serializing objects.
 *
 * @author Chris Bono
 * @since 1.2.0
 */
public interface JSONSchemaUtil {

	/**
	 * Create a new JSON schema that uses the provided object mapper to de/serialize
	 * objects of the specified type.
	 * @param objectType the type of objects the resulting schema represents
	 * @param objectMapper the mapper used to read and write objects from JSON
	 * @param <T> the type of objects the resulting schema represents
	 * @return the schema instance
	 */
	static <T> JSONSchema<T> schemaForTypeWithObjectMapper(Class<T> objectType, ObjectMapper objectMapper) {
		return JSONSchemaUtil.schemaForTypeWithObjectMapper(objectType, objectMapper, (b) -> {
		});
	}

	/**
	 * Create a new JSON schema that uses the provided object mapper to de/serialize
	 * objects of the specified type.
	 * @param objectType the type of objects the resulting schema represents
	 * @param objectMapper the mapper used to read and write objects from JSON
	 * @param schemaDefinitionBuilderCustomizer the schema definition builder customizer
	 * @param <T> the type of objects the resulting schema represents
	 * @return the schema instance
	 */
	static <T> JSONSchema<T> schemaForTypeWithObjectMapper(Class<T> objectType, ObjectMapper objectMapper,
			Consumer<SchemaDefinitionBuilder<T>> schemaDefinitionBuilderCustomizer) {
		var reader = new CustomJacksonJsonReader<>(objectMapper, objectType);
		var writer = new CustomJacksonJsonWriter<T>(objectMapper);
		var schemaDefinitionBuilder = new SchemaDefinitionBuilderImpl<T>().withPojo(objectType)
			.withSchemaReader(reader)
			.withSchemaWriter(writer);
		schemaDefinitionBuilderCustomizer.accept(schemaDefinitionBuilder);
		return JSONSchema.of(schemaDefinitionBuilder.build());
	}

	/**
	 * Reader implementation for reading objects from JSON using a custom
	 * {@code ObjectMapper}.
	 *
	 * @param <T> object type to read
	 */
	class CustomJacksonJsonReader<T> implements SchemaReader<T> {

		private static final LogAccessor LOG = new LogAccessor(CustomJacksonJsonReader.class);

		private final ObjectReader objectReader;

		private final Class<T> objectType;

		CustomJacksonJsonReader(ObjectMapper objectMapper, Class<T> objectType) {
			Assert.notNull(objectMapper, "objectMapper must not be null");
			Assert.notNull(objectType, "objectType must not be null");
			this.objectReader = objectMapper.readerFor(objectType);
			this.objectType = objectType;
		}

		@Override
		public T read(byte[] bytes, int offset, int length) {
			try {
				return this.objectReader.readValue(bytes, offset, length);
			}
			catch (IOException e) {
				throw new SchemaSerializationException(e);
			}
		}

		@Override
		public T read(InputStream inputStream) {
			try {
				return this.objectReader.readValue(inputStream, this.objectType);
			}
			catch (IOException e) {
				throw new SchemaSerializationException(e);
			}
			finally {
				try {
					inputStream.close();
				}
				catch (IOException e) {
					LOG.error(e, () -> "Failed to close input stream on read");
				}
			}
		}

	}

	/**
	 * Writer implementation for writing objects as JSON using a custom
	 * {@code ObjectMapper}.
	 *
	 * @param <T> object type to write
	 */
	class CustomJacksonJsonWriter<T> implements SchemaWriter<T> {

		private final ObjectMapper objectMapper;

		CustomJacksonJsonWriter(ObjectMapper objectMapper) {
			Assert.notNull(objectMapper, "objectMapper must not be null");
			this.objectMapper = objectMapper;
		}

		@Override
		public byte[] write(T message) {
			try {
				return this.objectMapper.writeValueAsBytes(message);
			}
			catch (JsonProcessingException e) {
				throw new SchemaSerializationException(e);
			}
		}

	}

}

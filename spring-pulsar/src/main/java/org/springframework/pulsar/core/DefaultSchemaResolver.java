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

package org.springframework.pulsar.core;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.ResolvableType;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.annotation.PulsarMessage;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Default schema resolver capable of handling basic message types.
 *
 * <p>
 * Additional message types can be configured with
 * {@link #addCustomSchemaMapping(Class, Schema)}.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 * @author Aleksei Arsenev
 */
public class DefaultSchemaResolver implements SchemaResolver, BeanClassLoaderAware {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private static final Map<Class<?>, Schema<?>> BASE_SCHEMA_MAPPINGS = new HashMap<>();
	static {
		BASE_SCHEMA_MAPPINGS.put(byte[].class, Schema.BYTES);
		BASE_SCHEMA_MAPPINGS.put(ByteBuffer.class, Schema.BYTEBUFFER);
		BASE_SCHEMA_MAPPINGS.put(ByteBuffer.allocate(0).getClass(), Schema.BYTEBUFFER);
		BASE_SCHEMA_MAPPINGS.put(ByteBuffer.allocateDirect(0).getClass(), Schema.BYTEBUFFER);
		BASE_SCHEMA_MAPPINGS.put(String.class, Schema.STRING);
		BASE_SCHEMA_MAPPINGS.put(Boolean.class, Schema.BOOL);
		BASE_SCHEMA_MAPPINGS.put(boolean.class, Schema.BOOL);
		BASE_SCHEMA_MAPPINGS.put(Byte.class, Schema.INT8);
		BASE_SCHEMA_MAPPINGS.put(byte.class, Schema.INT8);
		BASE_SCHEMA_MAPPINGS.put(Short.class, Schema.INT16);
		BASE_SCHEMA_MAPPINGS.put(short.class, Schema.INT16);
		BASE_SCHEMA_MAPPINGS.put(Integer.class, Schema.INT32);
		BASE_SCHEMA_MAPPINGS.put(int.class, Schema.INT32);
		BASE_SCHEMA_MAPPINGS.put(Long.class, Schema.INT64);
		BASE_SCHEMA_MAPPINGS.put(long.class, Schema.INT64);
		BASE_SCHEMA_MAPPINGS.put(Float.class, Schema.FLOAT);
		BASE_SCHEMA_MAPPINGS.put(float.class, Schema.FLOAT);
		BASE_SCHEMA_MAPPINGS.put(Double.class, Schema.DOUBLE);
		BASE_SCHEMA_MAPPINGS.put(double.class, Schema.DOUBLE);
		BASE_SCHEMA_MAPPINGS.put(Date.class, Schema.DATE);
		BASE_SCHEMA_MAPPINGS.put(Time.class, Schema.TIME);
		BASE_SCHEMA_MAPPINGS.put(Timestamp.class, Schema.TIMESTAMP);
		BASE_SCHEMA_MAPPINGS.put(Instant.class, Schema.INSTANT);
		BASE_SCHEMA_MAPPINGS.put(LocalDate.class, Schema.LOCAL_DATE);
		BASE_SCHEMA_MAPPINGS.put(LocalDateTime.class, Schema.LOCAL_DATE_TIME);
		BASE_SCHEMA_MAPPINGS.put(LocalTime.class, Schema.LOCAL_TIME);
	}

	private final Map<String, Schema<?>> customSchemaMappings = new LinkedHashMap<>();

	private final PulsarMessageAnnotationRegistry pulsarMessageAnnotationRegistry = new PulsarMessageAnnotationRegistry();

	private boolean usePulsarMessageAnnotations = true;

	private @Nullable ObjectMapper objectMapper;

	private @Nullable ClassLoader classLoader;

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	/**
	 * Sets whether to inspect message classes for the
	 * {@link PulsarMessage @PulsarMessage} annotation during schema resolution.
	 * @param usePulsarMessageAnnotations whether to inspect messages for the annotation
	 */
	public void usePulsarMessageAnnotations(boolean usePulsarMessageAnnotations) {
		this.usePulsarMessageAnnotations = usePulsarMessageAnnotations;
	}

	/**
	 * Adds a custom mapping from message type to schema.
	 * @param messageType the message type
	 * @param schema the schema to use for messages of type {@code messageType}
	 * @return the previously mapped schema or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable public Schema<?> addCustomSchemaMapping(Class<?> messageType, Schema<?> schema) {
		return this.customSchemaMappings.put(this.toMessageTypeMapKey(messageType), schema);
	}

	/**
	 * Removes the custom mapping from message type to schema.
	 * @param messageType the message type
	 * @return the previously mapped schema or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable public Schema<?> removeCustomMapping(Class<?> messageType) {
		return this.customSchemaMappings.remove(this.toMessageTypeMapKey(messageType));
	}

	/**
	 * Gets the currently registered custom mapping for the specified message type.
	 * @return optional custom topic registered for the message type
	 * @deprecated deprecated in favor of {@link #getCustomSchemaMapping(Class)} (Class)}
	 */
	@Deprecated(since = "1.2.5", forRemoval = true)
	public Map<Class<?>, Schema<?>> getCustomSchemaMappings() {
		return this.customSchemaMappings.entrySet()
			.stream()
			.collect(Collectors.toMap((e) -> this.fromMessageTypeMapKey(e.getKey()), Entry::getValue));
	}

	/**
	 * Gets the currently registered custom mapping for the specified message type.
	 * @param messageType the message type
	 * @return optional custom topic registered for the message type
	 */
	public Optional<Schema<?>> getCustomSchemaMapping(Class<?> messageType) {
		return Optional.ofNullable(this.customSchemaMappings.get(this.toMessageTypeMapKey(messageType)));
	}

	@Override
	public <T> Resolved<Schema<T>> resolveSchema(@Nullable Class<?> messageClass, boolean returnDefault) {
		if (messageClass == null) {
			return Resolved.failed("Schema must be specified when the message is null");
		}
		Schema<?> schema = BASE_SCHEMA_MAPPINGS.get(messageClass);
		if (schema == null) {
			schema = getCustomSchemaOrMaybeDefault(messageClass, returnDefault);
		}
		if (schema == null) {
			return Resolved.failed("Schema not specified and no schema found for " + messageClass);
		}
		return Resolved.of(castToType(schema));
	}

	@Nullable protected Schema<?> getCustomSchemaOrMaybeDefault(Class<?> messageClass, boolean returnDefault) {
		// Check for custom schema mapping
		var schema = this.getCustomSchemaMapping(messageClass).orElse(null);

		// If no custom schema mapping found, look for @PulsarMessage (if enabled)
		if (this.usePulsarMessageAnnotations && schema == null && messageClass != null) {
			schema = getAnnotatedSchemaType(messageClass);
			if (schema != null) {
				this.addCustomSchemaMapping(messageClass, schema);
			}
		}

		// If still no schema, possibly return a default
		if (schema == null && returnDefault) {
			if (messageClass != null) {
				try {
					return jsonSchemaForMessageType(messageClass);
				}
				catch (Exception e) {
					this.logger.debug(e, "Failed to create JSON schema for " + messageClass.getName());
				}
			}
			return Schema.BYTES;
		}
		return schema;
	}

	@Nullable
	// VisibleForTesting
	Schema<?> getAnnotatedSchemaType(Class<?> messageClass) {
		PulsarMessage annotation = this.pulsarMessageAnnotationRegistry.getAnnotationFor(messageClass).orElse(null);
		if (annotation == null || annotation.schemaType() == SchemaType.NONE) {
			return null;
		}
		var schemaType = annotation.schemaType();
		if (schemaType != SchemaType.KEY_VALUE) {
			return resolveSchema(annotation.schemaType(), messageClass, null).value().orElse(null);
		}
		// handle complicated key value
		var messageKeyClass = annotation.messageKeyType();
		Assert.state(messageKeyClass != Void.class,
				"messageKeyClass can not be Void.class when using KEY_VALUE schema type");

		var messageValueSchemaType = annotation.messageValueSchemaType();
		Assert.state(messageValueSchemaType != SchemaType.NONE && messageValueSchemaType != SchemaType.KEY_VALUE,
				() -> "messageValueSchemaType can not be NONE or KEY_VALUE when using KEY_VALUE schema type");

		Schema<?> keySchema = this.resolveSchema(messageKeyClass).orElseThrow();
		Schema<?> valueSchema = this.resolveSchema(messageValueSchemaType, messageClass, null).orElseThrow();
		return Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Resolved<Schema<T>> resolveSchema(SchemaType schemaType, @Nullable ResolvableType messageType) {
		try {
			Schema<?> schema = switch (schemaType) {
				case STRING -> Schema.STRING;
				case BOOLEAN -> Schema.BOOL;
				case INT8 -> Schema.INT8;
				case INT16 -> Schema.INT16;
				case INT32 -> Schema.INT32;
				case INT64 -> Schema.INT64;
				case FLOAT -> Schema.FLOAT;
				case DOUBLE -> Schema.DOUBLE;
				case DATE -> Schema.DATE;
				case TIME -> Schema.TIME;
				case TIMESTAMP -> Schema.TIMESTAMP;
				case BYTES -> Schema.BYTES;
				case INSTANT -> Schema.INSTANT;
				case LOCAL_DATE -> Schema.LOCAL_DATE;
				case LOCAL_TIME -> Schema.LOCAL_TIME;
				case LOCAL_DATE_TIME -> Schema.LOCAL_DATE_TIME;
				case JSON -> jsonSchemaForMessageType(requireNonNullMessageClass(schemaType, messageType));
				case AVRO -> AvroSchema.of(requireNonNullMessageClass(schemaType, messageType));
				case PROTOBUF -> {
					// WARN! Leave GeneratedMessageV3 fully-qualified as the dependency is
					// optional
					Class<?> messageClass = requireNonNullMessageClass(schemaType, messageType);
					yield ProtobufSchema.of((Class<? extends com.google.protobuf.GeneratedMessageV3>) messageClass);
				}
				case KEY_VALUE -> {
					yield getMessageKeyValueSchema(requireNonNullMessageType(schemaType, messageType));
				}
				case AUTO_CONSUME -> Schema.AUTO_CONSUME();
				case NONE -> {
					if (messageType == null || messageType.getRawClass() == null) {
						yield Schema.BYTES;
					}
					if (KeyValue.class.isAssignableFrom(messageType.getRawClass())) {
						yield getMessageKeyValueSchema(messageType);
					}
					yield resolveSchema(messageType.getRawClass(), true).orElseThrow();
				}
				default -> throw new IllegalArgumentException("Unsupported schema type: " + schemaType.name());
			};
			Assert.notNull(schema, "Unable to resolve schema type: " + schemaType.name());
			return Resolved.of(castToType(schema));
		}
		catch (RuntimeException e) {
			return Resolved.failed(e);
		}
	}

	private <T> JSONSchema<T> jsonSchemaForMessageType(Class<T> messageType) {
		if (this.objectMapper != null) {
			return JSONSchemaUtil.schemaForTypeWithObjectMapper(messageType, this.objectMapper);
		}
		return JSONSchema.of(messageType);
	}

	private Class<?> requireNonNullMessageClass(SchemaType schemaType, @Nullable ResolvableType messageType) {
		var nonNullMessageType = requireNonNullMessageType(schemaType, messageType);
		return Objects.requireNonNull(nonNullMessageType.getRawClass(),
				"Unable to determine raw class for " + nonNullMessageType);
	}

	private ResolvableType requireNonNullMessageType(SchemaType schemaType, @Nullable ResolvableType messageType) {
		return Objects.requireNonNull(messageType, "messageType must be specified for " + schemaType.name());
	}

	private Schema<?> getMessageKeyValueSchema(ResolvableType messageType) {
		Class<?> keyClass = messageType.resolveGeneric(0);
		Class<?> valueClass = messageType.resolveGeneric(1);
		Schema<?> keySchema = this.resolveSchema(keyClass).orElseThrow();
		Schema<?> valueSchema = this.resolveSchema(valueClass).orElseThrow();
		return Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
	}

	@SuppressWarnings("unchecked")
	private <X> Schema<X> castToType(Schema<?> rawSchema) {
		return (Schema<X>) rawSchema;
	}

	private Class<?> fromMessageTypeMapKey(String messageTypeKey) {
		try {
			return ClassUtils.forName(messageTypeKey, this.classLoader);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private String toMessageTypeMapKey(Class<?> messageType) {
		return messageType.getName();
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

}

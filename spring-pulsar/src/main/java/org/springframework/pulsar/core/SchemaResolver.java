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

import java.util.Objects;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.ResolvableType;
import org.springframework.lang.Nullable;

/**
 * Resolves schema to use for message types.
 *
 * @author Chris Bono
 */
public interface SchemaResolver {

	/**
	 * Get the schema to use for a particular message.
	 * @param <T> the schema type
	 * @param message the message instance
	 * @return the schema to use or {@code null} if no schema could be resolved
	 */
	default <T> Resolved<Schema<T>> resolveSchema(@Nullable T message) {
		return resolveSchema(message == null ? null : message.getClass());
	}

	/**
	 * Get the schema to use for a message type.
	 * @param <T> the schema type
	 * @param messageType the message type
	 * @return the schema to use or {@code null} if no schema could be resolved
	 */
	default <T> Resolved<Schema<T>> resolveSchema(@Nullable Class<?> messageType) {
		return resolveSchema(messageType, true);
	}

	/**
	 * Get the schema to use for a message type.
	 * @param <T> the schema type
	 * @param messageType the message type
	 * @param returnDefault whether to return default schema if no schema could be
	 * resolved
	 * @return the schema to use or the default schema if no schema could be resolved and
	 * {@code returnDefault} is {@code true} - otherwise {@code null}
	 */
	<T> Resolved<Schema<T>> resolveSchema(@Nullable Class<?> messageType, boolean returnDefault);

	/**
	 * Get the schema to use given a schema type and a message type.
	 * @param <T> the schema type
	 * @param schemaType the schema type
	 * @param messageType the message type
	 * @return the schema to use
	 */
	<T> Resolved<Schema<T>> resolveSchema(SchemaType schemaType, @Nullable ResolvableType messageType);

	/**
	 * Get the schema to use given a schema type and schema type information.
	 * @param <T> the schema type
	 * @param schemaType schema type
	 * @param messageType message type (not required for primitive schema types)
	 * @param messageKeyType message key type (must be specified when schema type is
	 * {@code KEY_VALUE})
	 * @return the schema to use
	 */
	default <T> Resolved<Schema<T>> resolveSchema(SchemaType schemaType, @Nullable Class<?> messageType,
			@Nullable Class<?> messageKeyType) {
		Objects.requireNonNull(schemaType, "schemaType must not be null");
		ResolvableType resolvableType = null;
		if (schemaType.isStruct()) {
			if (messageType == null) {
				return Resolved.failed("messageType must be specified for %s schema type".formatted(schemaType.name()));
			}
			resolvableType = ResolvableType.forClass(messageType);
		}
		else if (schemaType == SchemaType.KEY_VALUE) {
			if (messageType == null) {
				return Resolved.failed("messageType must be specified for KEY_VALUE schema type");
			}
			if (messageKeyType == null) {
				return Resolved.failed("messageKeyType must be specified for KEY_VALUE schema type");
			}
			resolvableType = ResolvableType.forClassWithGenerics(KeyValue.class, messageKeyType, messageType);
		}
		else if (schemaType == SchemaType.NONE) {
			if (messageType != null && messageKeyType != null) {
				resolvableType = ResolvableType.forClassWithGenerics(KeyValue.class, messageKeyType, messageType);
			}
			else if (messageType != null) {
				resolvableType = ResolvableType.forClass(messageType);
			}
		}
		return resolveSchema(schemaType, resolvableType);
	}

	/**
	 * Callback interface that can be implemented by beans wishing to customize the schema
	 * resolver before it is fully initialized, in particular to tune its configuration.
	 *
	 * @param <T> the type of the {@link SchemaResolver}
	 * @author Chris Bono
	 */
	@FunctionalInterface
	interface SchemaResolverCustomizer<T extends SchemaResolver> {

		/**
		 * Customize the schema resolver.
		 * @param schemaResolver the schema resolver to customize
		 */
		void customize(T schemaResolver);

	}

}

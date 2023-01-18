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

import org.apache.pulsar.client.api.Schema;

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
	@Nullable
	default <T> Schema<T> getSchema(T message) {
		return getSchema(message.getClass());
	}

	/**
	 * Get the schema to use for a message type.
	 * @param <T> the schema type
	 * @param messageType the message type
	 * @return the schema to use or {@code null} if no schema could be resolved
	 */
	@Nullable
	default <T> Schema<T> getSchema(Class<?> messageType) {
		return getSchema(messageType, true);
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
	@Nullable
	<T> Schema<T> getSchema(Class<?> messageType, boolean returnDefault);

}

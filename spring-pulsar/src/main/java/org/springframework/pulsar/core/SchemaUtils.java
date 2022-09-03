/*
 * Copyright 2022 the original author or authors.
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

/**
 * Utility class for Pulsar schema inference.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public final class SchemaUtils {

	private SchemaUtils() {
	}

	public static <T> Schema<T> getSchema(T message) {
		return getSchema(message.getClass());
	}

	public static <T> Schema<T> getSchema(Class<?> messageClass) {
		return getSchema(messageClass, true);
	}

	@SuppressWarnings("unchecked")
	public static <T> Schema<T> getSchema(Class<?> messageClass, boolean returnDefault) {
		return switch (messageClass.getName()) {
			case "java.lang.String" -> (Schema<T>) Schema.STRING;
			case "[B" -> (Schema<T>) Schema.BYTES;
			case "java.lang.Byte", "byte" -> (Schema<T>) Schema.INT8;
			case "java.lang.Short", "short" -> (Schema<T>) Schema.INT16;
			case "java.lang.Integer", "int" -> (Schema<T>) Schema.INT32;
			case "java.lang.Long", "long" -> (Schema<T>) Schema.INT64;
			case "java.lang.Boolean", "boolean" -> (Schema<T>) Schema.BOOL;
			case "java.nio.ByteBuffer" -> (Schema<T>) Schema.BYTEBUFFER;
			case "java.util.Date" -> (Schema<T>) Schema.DATE;
			case "java.lang.Double", "double" -> (Schema<T>) Schema.DOUBLE;
			case "java.lang.Float", "float" -> (Schema<T>) Schema.FLOAT;
			case "java.time.Instant" -> (Schema<T>) Schema.INSTANT;
			case "java.time.LocalDate" -> (Schema<T>) Schema.LOCAL_DATE;
			case "java.time.LocalDateTime" -> (Schema<T>) Schema.LOCAL_DATE_TIME;
			case "java.time.LocalTime" -> (Schema<T>) Schema.LOCAL_TIME;
			default -> (returnDefault ? (Schema<T>) Schema.BYTES : null);
		};
	}

}

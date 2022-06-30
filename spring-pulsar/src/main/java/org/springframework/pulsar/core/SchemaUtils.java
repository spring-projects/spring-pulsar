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
 * @author Soby Chacko
 */
public class SchemaUtils {

	@SuppressWarnings("unchecked")
	public static <T>  Schema<T> getSchema(T message) {
		if (message.getClass() == byte[].class) {
			return (Schema<T>) Schema.BYTES;
		}
		else if (message.getClass() == String.class) {
			return (Schema<T>) Schema.STRING;
		}
		else if (message.getClass() == Integer.class) {
			return (Schema<T>) Schema.INT32;
		}
		return (Schema<T>) Schema.BYTES;
	}
}

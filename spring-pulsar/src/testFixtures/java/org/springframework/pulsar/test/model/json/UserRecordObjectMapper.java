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

package org.springframework.pulsar.test.model.json;

import org.springframework.pulsar.test.model.UserRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Constructs custom {@link ObjectMapper} instances that leverage the
 * {@link UserRecordSerializer} and {@link UserRecordDeserializer}.
 */
public final class UserRecordObjectMapper {

	private UserRecordObjectMapper() {
	}

	public static ObjectMapper withSer() {
		var objectMapper = new ObjectMapper();
		var module = new SimpleModule();
		module.addSerializer(UserRecord.class, new UserRecordSerializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	public static ObjectMapper withDeser() {
		var objectMapper = new ObjectMapper();
		var module = new SimpleModule();
		module.addDeserializer(UserRecord.class, new UserRecordDeserializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	public static ObjectMapper withSerAndDeser() {
		var objectMapper = new ObjectMapper();
		var module = new SimpleModule();
		module.addSerializer(UserRecord.class, new UserRecordSerializer());
		module.addDeserializer(UserRecord.class, new UserRecordDeserializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

}

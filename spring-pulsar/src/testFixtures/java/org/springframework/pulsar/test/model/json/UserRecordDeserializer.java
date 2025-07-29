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

package org.springframework.pulsar.test.model.json;

import java.io.IOException;

import org.springframework.pulsar.test.model.UserRecord;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * Custom Jackson deserializer for {@link UserRecord}.
 *
 * @author Chris Bono
 * @since 1.2.0
 */
public class UserRecordDeserializer extends StdDeserializer<UserRecord> {

	public UserRecordDeserializer() {
		this(null);
	}

	public UserRecordDeserializer(Class<UserRecord> t) {
		super(t);
	}

	@Override
	public UserRecord deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
		JsonNode rootNode = jp.getCodec().readTree(jp);
		var name = rootNode.get("name").asText();
		var age = rootNode.get("age").asInt();
		return new UserRecord(name + "-deser", age + 5);
	}

}

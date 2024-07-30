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

import java.io.IOException;

import org.springframework.pulsar.test.model.UserRecord;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Custom Jackson serializer for {@link UserRecord}.
 *
 * @author Chris Bono
 * @since 1.2.0
 */
public class UserRecordSerializer extends StdSerializer<UserRecord> {

	public UserRecordSerializer() {
		this(null);
	}

	public UserRecordSerializer(Class<UserRecord> t) {
		super(t);
	}

	@Override
	public void serialize(UserRecord value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
		jgen.writeStartObject();
		jgen.writeStringField("name", value.name() + "-ser");
		jgen.writeNumberField("age", value.age() + 10);
		jgen.writeEndObject();
	}

}

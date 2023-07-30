/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.annotation;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@DirtiesContext
public class PulsarMessageAnnotationScannerTests {

	@PulsarMessage(topic = "test", type = SchemaType.KEY_VALUE, messageKeyType = String.class)
	public static class Message1 {

	}

	@PulsarMessage(type = SchemaType.AVRO)
	public static class Message2 {

	}

	@PulsarMessage(topic = "test")
	public static class Message3 {

	}

	@Nested
	@ContextConfiguration(classes = BasicTestCases.Config.class)
	class BasicTestCases {

		@Test
		void resolveMessageSchema(@Autowired DefaultSchemaResolver resolver) {
			assertThat(resolver.resolveSchema(Message1.class).orElseThrow())
					.asInstanceOf(InstanceOfAssertFactories.type(KeyValueSchema.class)).satisfies((keyValueSchema -> {
						AssertionsForClassTypes.assertThat(keyValueSchema.getKeySchema()).isEqualTo(Schema.STRING);
						AssertionsForClassTypes.assertThat(keyValueSchema.getKeyValueEncodingType())
								.isEqualTo(KeyValueEncodingType.INLINE);
					}));
			assertThat(resolver.resolveSchema(Message2.class).orElseThrow()).isInstanceOf(AvroSchema.class);
			assertThat(resolver.resolveSchema(Message3.class).orElseThrow()).isInstanceOf(JSONSchema.class);
		}

		@Test
		void resolveMessageTopic(@Autowired DefaultTopicResolver resolver) {
			assertThat(resolver.resolveTopic("", Message1.class, () -> "default").orElseThrow()).isEqualTo("test");
			assertThat(resolver.resolveTopic("", Message2.class, () -> "default").orElseThrow()).isEqualTo("default");
			assertThat(resolver.resolveTopic("", Message1.class, () -> "default").orElseThrow()).isEqualTo("test");
		}

		@EnablePulsar
		@Configuration
		static class Config {

			@Bean
			public DefaultTopicResolver defaultTopicResolver() {
				return new DefaultTopicResolver();
			}

			@Bean
			public DefaultSchemaResolver defaultSchemaResolver() {
				return new DefaultSchemaResolver();
			}

		}

	}

}
